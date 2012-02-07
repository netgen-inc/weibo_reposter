var fs = require('fs');
var settings = require(__dirname + '/etc/settings.json');
var url = require('url');
//var de = require('devent').createDEvent('sender');
//var queue = require('queuer');
var logger = require('./lib/logger').logger(settings.logFile);
var util = require('util');
var event = require('events').EventEmitter;
var tool = require('./lib/tool');
var async = require('async');


//发送队列的API
//var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.send);

var Reposter = require('./lib/reposter').Reposter;
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
    return;
    if(status){
        de.emit('task-finished', task);  
    }else{
        de.emit('task-error', task);     
    }
}

reposter.on('repost', function(error, body, id, status, context){
    taskBack(context.task, complete(error, body, id, status, context));
    dequeue();
});


var repost = function(task, callback){
    db.getRepostTask(task.uri, function(err, weiboId, repostRecord){
        if(err){
            console.log(['fetch repost relation error', err]);
            //要转发的微博没有发送，并且超过1小时，放弃这个任务
            if(err.number == 7000 && tool.currentTimestap() - err.row.in_time > 3600){
                console.log("task " + task.uri + " timeout");
                taskBack(task, true);
            }else{
                taskBack(task, false);
            }
            return; 
        }
        
        var stockCode = repostRecord.stockCode;
        //debug模式下，总是使用stock0@netgen.com.cn发送微博
        if(settings.mode == 'debug'){
            stockCode = 'sz900000';  
        }
        stockCode = stockCode.toLowerCase();
        
        //微博账号错误
        if(!weiboAccounts[stockCode] || 
            !weiboAccounts[stockCode].access_token || 
            !weiboAccounts[stockCode].access_token_secret){
            logger.info("error\t" + blog.id + "\t" + stockCode + "\tNOT Found the account\t"); 
            taskBack(task, true);
            return;
        }
        reposter.repost(weiboId, '', weiboAccounts[stockCode], {task:task,record:repostRecord});
        callback();
    });
};

var dequeue = function(){
    return;
    if(aq.length() >= settings.reposterCount){
        return;
    }
    
    rq.dequeue(function(err, task){
        if(err == 'empty' || task == undefined){
            console.log('send queue is empty');
            return;
        }
        console.log(['dequeue', task]);
        aq.push(task);
    });
}

var start = function(){
    return;
    setInterval(function(){
        dequeue();    
    }, settings.queue.interval);  
    
    de.on('queued', function( queue ){
        if(queue == settings.queue.send){
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
    var record = context.record;
    var task = context.task;
    if(!error){
        logger.info("success\t" + record.id + "\t" + weiboId + "\t" + record.article_id + "\t" + body.id + "\t" + body.t_url);
        db.reposted(record.id, body.id, body.t_url, function(err, info){
            console.log([err, info]);    
        });
        return true;
    }
    
    var errMsg = error.error;
    logger.info("error\t" + record.id +"\t"+ weiboId + "\t" + record.stock_code + "\t" + errMsg);  
    
    //发送受限制
    if(errMsg.match(/^40(308|090)/)){
        return false;
    //40013太长, 40025重复
    }else if(errMsg.match(/^400(13|25)/)){                                                                                                                          
        return true;
    }else{
        if(task.retry >= settings.queue.retry){
            logger.info("error\t" + record.id +"\t"+ record.stock_code + "\t"+ "\tretry count more than "+settings.queue.retry);
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

/**
 * 测试代码
setTimeout(function(){
    var sender = new Sender();
    sender.init(settings);
    var task = {uri:'mysql://abc.com/stock_radar#1'};
    sender.on('send', function(error, body, blog, context){
        console.log(error);
        taskBack(context.task, complete(error, body, blog));
    });
    send(task, sender, {task:task});
}, 1000);

*/

setTimeout(function(){
    var task = {uri:'mysql://abc.com/stock_radar#1', retry:0};
    aq.push(task);
    console.log(aq.length());
}, 1000);




 
