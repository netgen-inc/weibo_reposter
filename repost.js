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


//发送队列的API
var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.repost);
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
    if(status){
        de.emit('task-finished', task);  
    }else{
        de.emit('task-error', task);     
    }
}


var repost = function(task, callback){
    db.getRepostTask(task.uri, function(err, weiboId, record){
        var context = {task:task,record:record};
        if(err){
            console.log(['fetch repost relation error', err]);
            //要转发的微博不存在或者没有发送，并且超过3小时，放弃这个任务
            if(err.number == 7000){
                if(tool.timestamp() - record.in_time > 10800){
                    complete(err, null, '', '', context);
                    taskBack(task, true);
                }
            }else{
                taskBack(task, complete(err, null, weiboId, '', context));
            }
            callback();
            dequeue();
            return; 
        }
        
        var stockCode = record.stock_code;
        //debug模式下，总是使用stock0@netgen.com.cn发送微博
        if(settings.mode == 'debug'){
            stockCode = 'sz900000';
        }
        stockCode = stockCode.toLowerCase();
        
        //微博账号错误
        if(!weiboAccounts[stockCode] || 
            !weiboAccounts[stockCode].access_token || 
            !weiboAccounts[stockCode].access_token_secret){
            logger.info("error\t" + record.id + "\t" + stockCode + "\tNOT Found the account\t"); 
            taskBack(task, true);
            callback();
            dequeue();
            return;
        }
        
        //限速，不再做任何处理，等到任务超时重新入队
        if(!sendAble(stockCode)){
            callback();
            dequeue();
            return;
        }

        var cb = function(error, body, id, status, context){
            taskBack(context.task, complete(error, body, id, status, context));
            callback();    
            dequeue();
        }
        context.user = weiboAccounts[stockCode];
        var status = '';
        if(record.content){
            status = record.content;
        } else if (record.title){
            status = record.title;    
        }
        reposter.repost(weiboId, status, weiboAccounts[stockCode], context, cb);
    });
};


//限速
var sendAbleList = {};
var sendAble = function(stockCode){
    if(typeof sendAbleList[stockCode] != 'object'){
        sendAbleList[stockCode] = {startTime:tool.timestamp(), cnt:0};
    }
    
    var cur = sendAbleList[stockCode];
    if(tool.timestamp() - cur.startTime <= settings.limit.interval){
        if(cur.cnt < settings.limit.max){
            cur.cnt += 1;
            return true;     
        }else{
            return false;   
        }
    }else{
        cur.cnt = 1;
        cur.startTime = tool.timestamp();
        return true;
    }
}

var dequeue = function(){
    console.log('local queue length is ' + aq.length());
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
        logger.info("success\t" + record.id + "\t" + record.stock_code + "\t" + weiboId + "\t" + body.id + "\t" + body.t_url);
        db.reposted(record, body.id, body.t_url, weiboId, context.user.id, function(err, info){
            if(err){
                console.log([err, info]);    
            }
        });
        return true;
    }
    
    var errMsg = error.error || error.message;
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
/*
process.on('uncaughtException', function(e){
    console.log(['unkonwn exception:', e]);
});
*/

console.log('reposter start at ' + tool.getDateString() + ', pid is ' + process.pid);

/**
 * 测试代码

setTimeout(function(){
    var task = {uri:'mysql://abc.com/dddd#1', retry:0};
    aq.push(task);
    console.log(aq.length());
}, 1000);
*/





 
