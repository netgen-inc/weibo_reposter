var fs = require('fs');
var settings = require(__dirname + '/etc/settings.json');
var url = require('url');
var de = require('devent').createDEvent('sender');
var queue = require('queuer');
var logger = require('./lib/logger').logger(settings.logFile);
var util = require('util');
var event = require('events').EventEmitter;
var tool = require('./lib/tool').tool;
var async = require('async');

var redis = require("redis");
var redisCli = redis.createClient(settings.redis.port, settings.redis.host);


//发送队列的API
var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.repost);
var Reposter = require('./lib/reposter_v3').Reposter;
var reposter = new Reposter();
reposter.init(settings);

//发送对象保存在该数组中
var senders = [];

var db = require('./lib/db').db;
db.init(settings);

//所有微博账号
var weiboAccounts = {};
db.loadAccounts(function(err, accounts){
    if(err){
        console.log('!!!load account error!!!');   
        return;
    }
    weiboAccounts = accounts;
    console.log('access token loaded');
    //由于发送依赖账号，所以必须先加载完账号才能开始处理发送请求
    console.log('starting dequeue');
    start();
});

var taskBack = function(task,  status){
    if(status){
        de.emit('task-finished', task);  
    }else{
        de.emit('task-error', task);     
    }
}

var repost = function(task, callback){
    db.getRepostTask(task.uri, function(err, record, sent){
        var context = {task:task,record:record};
        if(err){
            console.log(['fetch repost task error', record, err]);
            //要转发的微博不存在或者没有发送，并且超过3小时，放弃这个任务
            err.nextAction = 'drop';
            complete(err, null, '', '', context);
            taskBack(task, true);
            callback();
            dequeue();
            return; 
        }

        if(tool.timestamp() - record.in_time > 21600){
            complete({number:8000,message:'waiting more than 6 hour'}, null, '', '', context);
            taskBack(task, true);
            callback();
            dequeue();
            return;
        }

        var status = '';
        if(record.content){
            status = record.content;
        } else if (record.title){
            status = record.title;    
        }

        if(status.match(/(公告|:|：|【|\[)/)){
            logger.info("error\t" + record.id + "\t" + "\tthe title is invalid"); 
            taskBack(task, true);
            callback();
            dequeue();
            return;
        }
        
        //debug模式下，总是使用stock0@netgen.com.cn发送微博
        if(settings.mode == 'debug'){
            record.stock_code= 'sz900000';
        }
        record.stock_code = record.stock_code.toLowerCase();

        
        //微博账号错误
        var account = getAccount(record.stock_code, sent);
        if(!account || !account.weibo_center_id){
            logger.info("error\t" + record.id + "\t" + record.stock_code+ "\tNOT Found the account\t"); 
            taskBack(task, true);
            callback();
            dequeue();
            return;
        }

        var cb = function(error, body, id, status, context){
            taskBack(context.task, complete(error, body, id, status, context));
            callback();    
            dequeue();
        }
        context.user = account;

        //限速，不再做任何处理，等到任务超时重新入队
        sendAble(record.stock_code, sent.weibo_id, account.id, function(err, result){
            if(err){
                if (err.msg == 'reposted') {
                    logger.info("error\trepeat\t" + sent.weibo_id+ "\t" + account.id);
                    taskBack(task, true);
                }
                callback();    
                dequeue();
            }else{
                reposter.repost(sent.weibo_id, status, account, context, cb);
            }
        });
    });
};

var getAccount = function (stockCode, sent) {
    if(settings.mode == 'debug') {
        stockCode = 'sz900000';
    }
    var sentAccount = weiboAccounts.ids[sent.account_id];
    if(!sentAccount) {
        return null;
    } 

    if(weiboAccounts.stocks[stockCode]) {
        return weiboAccounts.stocks[stockCode][sentAccount.provider];
    }
    return null;
}


//限速
var lockedAccounts = {};
var sendAble = function(stockCode, weiboId, accountId, callback){
    lockedAccounts[accountId] = accountId;
    var reposted = function (cb) {
        db.getRepostRecord(accountId, weiboId, function (err, result) {
            if(err || result.length != 0) {
                cb({msg:'reposted'});
            }else {
                cb();
            }
        });
    };

    var accountAble = function (cb) {
        var limited = function(lcb){
            var ts = tool.timestamp();
            var key = "SEND_LIMIT_" + accountId;
            redisCli.get(key, function(err, lastSend){
                if(!lastSend){
                    redisCli.setex(key, 180, ts);
                    lcb(null, true);
                }else{
                    lcb({msg:'limit'}, false);
                }
            });
        }   
        if(stockCode == 'a_stock'){
            redisCli.get('a_stock_counter', function(err, count){
                if(count > 0){
                    cb({msg:'limit_astock'}, false);
                }else{
                    limited(cb);    
                }
            });
        }else{
            limited(cb);
        }
    };
    async.series ([reposted, accountAble], function (err, result) {
        delete lockedAccounts[accountId];
        callback(err);
    });
}

var dequeue = function(){
    if(aq.length() >= settings.reposterCount){
        return;
    }
    
    rq.dequeue(function(err, task){
        if(err == 'empty' || task == undefined){
            console.log('repost queue is empty');
            return;
        }
        //console.log(['dequeue', task]);
        aq.push(task);
    });
}

var start = function(){
    setInterval(function(){
        dequeue();    
    }, settings.queue.interval);  
    
    de.on('queued', function( queue ){
        if(queue == settings.queue.repost){
            console.log( queue + "有内容");
            dequeue();
        }
    }); 
    console.log('sender start ok'); 
}

/**
 发送结束后的处理，返回true表示发送完成
*/
var complete = function(error, body, weiboId, status, context){
    var record = context.record || {};
    var task = context.task;
    if(!error){
        body.t_url = '';
        logger.info("success\t" + record.id + "\t" + record.stock_code + "\t" +context.user.id+ "\t" + weiboId + "\t" + body.id);
        db.reposted(record, body.id, body.t_url, weiboId, context.user.id, function(err, info){
            if(err){
                console.log([err, info]);    
            }
        });
        return true;
    }
    if(!error.error_code){
        error.error_code = '70000';
    }
    
    var errMsg = error.error || error.message;
    logger.info("error\t" + record.id +"\t"+ weiboId + "\t" + record.stock_code + "\t" + errMsg);  
    
    //发送受限制
    if(error.nextAction == 'delay' && task.retry < settings.queue.retry){
        return false;
    //40013太长, 40025重复
    }else{
        if(error.nextAction == 'drop' || task.retry >= settings.queue.retry){
            return true;
        }else{
            return false;
        }
    }
}

var aq = async.queue(repost, settings.reposterCount);

fs.writeFileSync(__dirname + '/server.pid', process.pid.toString(), 'ascii');

//收到进程信号重新初始化
process.on('SIGUSR2', function () {
    settings = require('./etc/settings.json');
    db.init(settings);
    for(i = 0; i < senders.length; i++){
        senders[i].init(settings);
    }
});
/*
process.on('uncaughtException', function(e){
    console.log(['unkonwn exception:', e]);
});
*/

console.log('reposter start at ' + tool.getDateString() + ', pid is ' + process.pid);

/**
 * 测试代码
setTimeout(function(){
    var task = {uri:'mysql://abc.com/dddd#142', retry:0};
    aq.push(task);
    console.log(aq.length());
}, 1000);
 */
