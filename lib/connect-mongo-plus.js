/******************************************************************************
   connect-mongo-plus

   Copyright (C) 2012 Eric des Courtis <eric.des.courtis@benbria.ca>

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to
   deal in the Software without restriction, including without limitation the
   rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
   sell copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in
   all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
   IN THE SOFTWARE.

   This is based on the connect-mongo modules by:
       Casey Banner <kcbanner@gmail.com>


      ..      ...  . MI M..     ..                 ..                           
              ... ,.M.. M7..M...                                                
              ..+8 M.  ....,N.            . .. . ..    ..                       
               .M  ..  ...  .M .DMM=7MMM?,MM87MM ..    ..                       
            ..$MM .         D=MZ. . . . + .   ..MM?MM8,.$M                      
              M ...      ...I:                    ..     .MMMM+                 
             MD:   ..  . . .D                                 O8.  .            
             M. .  =? ..O:  +                                  :MZ              
          . ..7M..,     ..M?.$MMO, .                             7+             
    .. ..NMMI+8MM..      ..M$:+M...~$. .                        .OO             
     .,MI..:I,,,=          $.=?::N...:?.                         . N+. .        
     M+ ..N:,MOM.          = ..~,,N. . $..                         N,IM8        
  ..M. ..7::M.M.. ,, . . ..7..  $:,,... M                          = . 7?.      
   M..  .I,D,?MNMN+~:$MO., N.    8:O..  ..                          ~.  M.      
  :M .  .+IM.M~          . O.    .M...  I.                          ... M  .    
  ,M .  .7M.Z:..         . $      ,.O8N~ .                          M   ZM .    
   .MN=?M7. M.           ..N                                      .D.   ..M8    
    .   ...~$        . N, .D                                        M.     M    
          .M.....    ..Z  Z,                                      ..,.  .  M    
           M.....    ...  .~M                                       O     M+    
          .MMMD8D8D888DDM,. ,  .                                  ?MM .M?  M    
    ..    ,M .N8D88D8M.      M .                                ..,M.  ..       
          .M   .DD8M .    .  ~..                                NNN$            
          .M  .  .D ..~  .   .                                  N..             
           :O ,...M..     ...+..                            :=ZM7               
    ..     .M..  .I.  .7   ..8.    ...  ....     .    .. . ~M. .                
    ..    .  M..   M. . .  .IZO.   .MD   .Z.  . OM::O$NM8MN.                    
             .M?   ..      DM$$$MMZ$$MMMM..=MMM$$$DDM$$$N .
              ..MM$MMMZ7NM?.M$$$MM$$$M..      MZ$$N8M$$$M.
  ....                     .M$$$MM$$$M.      .MZ$$N8M$$$M
  ....                     .M$$$MM$$$M .    . D8$$DDM$$$M.
                            8$$$DM$$$M       .7M$$OMN$$$N,.
                            ~8$$OM$$$M .       M$$ZM+O$$8=
    ..                     . M$$$MZ$$M..       M$$$M.M$$$O
    ..                      .MDD8MM$$8~.       $MMMM.MDDOM.
                           ..DMMMMMMMMD.       .MMMM,NMMMM.
                              MMNDNMMMN       . ++~: .MMMN   -= Dudebro =-
******************************************************************************/

var mongo = require('mongodb');
var url = require('url');

/*
        var db_config = {
            'servers': [         
                {'host': 'node-a', 'port': 27017, 'options': options_node_a},
                {'host': 'node-b', 'port': 27017, 'options': options_node_b},
            ],
            'db_name': 'nodeloop',
            'db_collection': 'sessions',
            'db_username': 'username',
            'db_password': 'password',
            'db_auth_options': auth_options,
            'db_remove_expired_interval': 60, // in minutes
            'db_options': db_options,
            'rs_options' : replset_options
        };
*/

module.exports = function (connect) {
    var Store = connect.session.Store;

    function MongoStore(options, callback) {
        options = options || {};
        Store.call(this, options);

        var servers = options.db_config.servers.map(
            function (server) {
                return new mongo.Server(server.host, server.port, server.options);
            }
        );

        function isAReplicaSet(servers) {
            return servers.length > 1;
        }

        var serverConfig;

        if (isAReplicaSet(servers)) {
            serverConfig = new mongo.ReplSetServers(servers, options.db_config.rs_options);
        } else {
            serverConfig = servers[0];
        }

        this.db = new mongo.Db(options.db_config.db_name, serverConfig, options.db_config.db_options);

        var self = this;
        this._get_collection = function (callback) {
            if (self.collection) {
                callback && callback(self.collection);
                return;
            }

            self.db.collection(
                options.db_config.db_collection,
                function (err, collection) {
                    if (err) {
                        throw new Error('Error getting collection in MongoStore: ' + self.db_collection);
                    }

                    self.collection = collection;

                    setInterval(
                        function () { self.collection.remove({expires: {$lte: new Date()}}); },
                        options.db_config.db_remove_expired_interval * 60 * 1000
                    );

                    callback && callback(self.collection);
                }
            );
        };

        this.db.open(
            function (err, db) {
                if (err) {
                    throw new Error('Error connecting to database');
                }

                if (options.db_config.db_username && options.db_config.db_password) {
                    db.authenticate(
                        options.db_config.db_username,
                        options.db_config.db_password,
                        options.db_config.db_auth_options || {},
                        function (err, isAuthenticated) {
                            if (!isAuthenticated) {
                                throw new Error('Error authenticating in MongoStore: ' + err);
                            }
                            self._get_collection(callback);
                        }
                    );
                    return;
                }

                self._get_collection(callback);
            }
        );
    };

    MongoStore.prototype.__proto__ = Store.prototype;

    MongoStore.prototype.get = function(session_id, callback) {
        var self = this;
        this._get_collection(
            function(collection) {
                collection.findOne({_id: session_id},
                    function(err, session) {
                        try {
                            if (err) {
                                callback && callback(err, null);
                                return;
                            }
 
                            if (session) {
                                if (!session.expires || new Date < session.expires) {
                                    //TODO: Find out if session needs to be deserialized
                                    callback(null, session.session);
                                    return;
                                }

                                self.destroy(session_id, callback);
                                return;
                            }

                            callback && callback();
                        } catch (err) {
                            callback && callback(err);
                        }
                    }
                );
            }
        );
    };

    MongoStore.prototype.set = function(session_id, session, callback) {
        try {
            //TODO: Find out if session needs to be serialized
            var s = {_id: session_id, session: session};

            if (session && session.cookie && session.cookie._expires) {
                s.expires = new Date(session.cookie._expires);
            }

            this._get_collection(
                function (collection) {
                    collection.update(
                        {_id: session_id},
                        s,
                        {upsert: true, safe: true}, 
                        function (err, data) {
                            if (err) {
                                callback && callback(err);
                                return;
                            }
                            callback && callback(null);
                        }
                    );
                }
            );
        } catch (err) {
          callback && callback(err);
        }        
    };

    MongoStore.prototype.destroy = function (session_id, callback) {
        this._get_collection(
            function (collection) {
                collection.remove(
                    {_id: session_id},
                    function () { callback && callback(); }
                );
            }
        );
    };

    MongoStore.prototype.length = function (callback) {
        this._get_collection(
            function(collection) {
                collection.count(
                    {}, 
                    function(err, count) {
                        if (err) {
                            callback && callback(err);
                            return;
                        }
                        callback && callback(null, count);
                    }
                );
            }
        );
    };

    MongoStore.prototype.clear = function (callback) {
        this._get_collection(
            function (collection) {
                collection.drop(
                    function () {
                        callback && callback();
                    }
                );
            }
        );
    };

    return MongoStore;
};

