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
    
    _self.repost = function(id, status, account, context, cb){
        status = status || '';
        var to = setTimeout(function(){
            var error = {error_code:0, error:'request timeout'};
            _self.emit('send', error, null, id, status, context);
        }, settings.weibo.timeout);
        var data = {weiboId:id, status:status, accountId:account.weibo_center_id,fromApp:settings.weiboCenter.appName, sync:1};
        var url = settings.weiboCenter.urlRoot + '/weibo/repost';
        hihttp.post(url, data, function(err, body, response){
            console.log(body);
            clearTimeout(to);
            var error = null;
            if (err) {
                error = {error_code : err.code || 7000, error: err.msg || 'reqest error', nextAction:'retry'};
            } 
            
            if(body && body.result != 'SUCCESS'){
                error = {error_code:body.error.code, error:body.error.msg, nextAction:body.error.nextAction};
            }
            cb(error, body, id, status, context);
            _self.running = false;
        });
    }
}
util.inherits(Reposter, event);
exports.Reposter = Reposter ;
