var util = require('util');
var event = require('events').EventEmitter;
var weibo = require('weibo');
var Reposter = function(){
    var _self = this;
    _self.running = false;
    var settings, db;
    
    _self.init = function(configs){
        settings = configs;
        weibo.init('tsina', settings.weibo.appkey, settings.weibo.secret);
    }
    
    _self.repost = function(id, status, account, context){
        status = status || '';
        fixAccount(account);
        var to = setTimeout(function(){
            var error = {statusCode:0, error:'request timeout'};
            _self.emit('send', error, null, context);
        }, settings.weibo.timeout);
        var data = {id:id, status:status,user:account};
        weibo.tapi.repost(data, function(err, body, response){
            clearTimeout(to);
            if(typeof body == 'string'){
                body = JSON.parse(body);   
            }
            
            _self.running = false;
            
            var error = null;
            if(err){
                error = err.message;
                console.log(error);
            }
           
            _self.emit('repost', error, body, id, status, context);
        });
    }
    
    var fixAccount = function(account){
        account.blogtype = 'tsina';
        account.authtype = 'oauth';
        account.oauth_token_key = account.access_token;
        account.oauth_token_secret = account.access_token_secret;
    }
}
util.inherits(Reposter, event);
exports.Reposter = Reposter ;


