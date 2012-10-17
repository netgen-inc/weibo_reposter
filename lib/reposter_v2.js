var util = require('util');
var hihttp = require('./hihttp');
var event = require('events').EventEmitter;
var Reposter = function(){
    var _self = this;
    _self.running = false;
    var settings, db;
    
    _self.init = function(configs){
        settings = configs;
    }

    _self.getParams = function(params, account) {
        params = params || {};
        params.oauth_consumer_key = settings.weibo.appkey;
        params.oauth_version = '2.a';
        params.scope = 'all';
        params.access_token = account.access_token;

        if(!params.clientip) {
            params.clientip = '127.0.0.1';
        }
        return params;
    };
    
    _self.repost = function(id, status, account, context, cb){
        status = status || '';
        var to = setTimeout(function(){
            var error = {statusCode:0, error:'request timeout'};
            _self.emit('send', error, null, context);
        }, settings.weibo.timeout);
        var data = {id:id, status:status};
        data = _self.getParams(data, account);
        var url = 'https://api.weibo.com/2/statuses/repost.json';
        hihttp.post(url, data, function(err, body, response){
            clearTimeout(to);
            if(typeof body == 'string'){
                body = JSON.parse(body);   
            }
            
            _self.running = false;
            if(body && body.error_code){
                err = body;
            }
            cb(err, body, id, status, context);
        });
    }
}
util.inherits(Reposter, event);
exports.Reposter = Reposter ;
