var mysql = require('mysql'); 
var url = require('url');
var async = require('async');
var tool = require('../lib/tool').tool;
var dbSpider;
var dbWeibo;

var Db = function(){
    var _self = this;
    var settings;
    var cli;
    _self.init = function(configs){
        settings = configs;
        dbSpider = mysql.createClient(settings.mysql.spider);
        dbWeibo = mysql.createClient(settings.mysql.weibo);
    }   
    
    _self.loadAccounts = function(cb){
        var sql = "SELECT * FROM account";
        dbWeibo.query(sql, function(err, results){
            if(err){
                cb(err, null);
                return;
            }
            var as = {ids : {}, stocks : {}};
            for(var i = 0; i < results.length; i++){
                var wa = results[i];
                as.ids[wa.id] = wa;
                if(!as.stocks[wa.stock_code]) {
                    as.stocks[wa.stock_code] = {};
                }
                as.stocks[wa.stock_code][wa.provider] = wa;
            }
            cb(null, as);
        });
    }
    
    /**
     * 
     */
    _self.getArticleStock = function(id, cb){
        var sql = "SELECT * FROM article_stock WHERE id = ? AND repost_status = 0";
        dbSpider.query(sql, [id], function(err, results){
            if(err){
                cb(err, results);        
            }else{
                if(results.length != 1){
                    cb({number:7001,message:'not found article_stock id ' + id});
                }else{
                    cb(null, results[0]);   
                }
            }
            
        });
    }

    _self.getTask = function(id, cb){
        var sql = "SELECT * FROM repost_task WHERE id = ? AND repost_status = 0";
        dbWeibo.query(sql, [id], function(err, results){
            if(err){
                cb(err, results);        
            }else{
                if(results.length != 1){
                    cb({number:7001,message:'not found repost_task id ' + id});
                }else{
                    cb(null, results[0]);   
                }
            }
            
        });
    }
    
    _self.getMicroBlog = function(articleStock, cb){
        var sql = "SELECT * FROM micro_blog WHERE article_id = ?";
        dbWeibo.query(sql, [articleStock.article_id], function(err, results){
            if(err){
                cb(err, results);   
            }else{
                if(results.length != 1){
                    cb({number:7002,message:'not found micro_blog by article_id ' + articleStock.article_id,row:results[0]});
                }else if(results[0].status == 1){
                    cb(err, results[0]);
                }else{
                    //要转发的微博没有发送
                    var err = {number:7000,message:'the micro blog will be repost has not send',row:results[0]};
                    cb(err, results[0]);
                }
            }
        });
    }

    _self.getArticleTitle = function(repostTask, cb){
        repostTask.title = repostTask.content;
        cb(null, repostTask);
    }
    
    _self.getSentMicroBlog = function(id, cb){
        var sql = "SELECT * FROM sent_micro_blog WHERE id = ? AND deleted = 0";
        dbWeibo.query(sql, [id], function(err, results){
            if(err){
                cb(err);   
            }else{
                if(results.length != 1){
                    var err = {number:7000, message:"the micro blog will be repost not exists"};
                    cb(err, null); 
                }else{
                    cb(null, results[0]); 
                }
            }
        });
    }
    
    //通过转发任务uri获取已发微博的id
    _self.getRepostTask = function(uri, callback){
        var uri = url.parse(uri);
        var ids = uri.hash.substring(1).split('_');
        if(ids.length != 2 || !ids[0] || !ids[1]) {
            cb({message:'bad task uri'});
            return;
        } 


        var  taskId = ids[0], sentId = ids[1];

        _self.getTask(taskId, function(err, task){
            if(err) {
                callback(err, task);
                return;
            } 

            task.title = task.content;    
            _self.getSentMicroBlog(sentId, function (err, result) {
                if (err) {
                    callback(err, result);
                    return;
                }
                callback(null, task, result);
            });
        });
    }
    
    _self.reposted = function(record, weiboId, weiboUrl, repostedWeiboId, accountId, cb){
        var sql = "INSERT INTO reposted_micro_blog(article_id,stock_code,repost_time,weibo_id,weibo_url,reposted_weibo_id, account_id)"
                   + " VALUES(?, ?, ?, ?, ?, ?, ?)";
        var data = [0, record.stock_code, tool.timestamp(), weiboId, weiboUrl, repostedWeiboId, accountId];
        
        dbWeibo.query(sql, data, function(err, info){
            cb(err, info);
        });
        
    };

    _self.getRepostRecord = function (accountId, repostedWeiboId, callback) {
        var sql = "SELECT * FROM reposted_micro_blog WHERE reposted_weibo_id = ? AND account_id = ?";
        sql = dbWeibo.format(sql, [repostedWeiboId, accountId]);
        dbWeibo.query(sql, callback);

    };
}
exports.db = new Db();
