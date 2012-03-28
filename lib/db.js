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
        var expired = [], accounts = {};
        dbWeibo.query(sql, function(err, results){
            if(err){
                cb(err, null);
                return;
            }
            
            weiboAccounts = {};
            for(var i = 0; i < results.length;i++){
                var wa = results[i];
                if(wa.block_id && wa.block_id > 0){
                    continue;
                }
                if(!wa.access_token || !wa.access_token_secret){
                    console.log('No access_token:' + wa.stock_code);
                }
                wa.blogtype = 'tsina';
                wa.authtype = 'oauth';
                wa.oauth_token_key = wa.access_token;
                wa.oauth_token_secret = wa.access_token_secret;
                weiboAccounts[results[i].stock_code] = wa;
            }
            cb(null, weiboAccounts);
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
    
    _self.getSentMicroBlog = function(taskRecord, cb){
        var sql = "SELECT * FROM sent_micro_blog WHERE micro_blog_id = ? AND deleted = 0";
        dbWeibo.query(sql, [taskRecord.micro_blog_id], function(err, results){
            if(err){
                cb(err);   
            }else{
                if(results.length != 1){
                    var err = {number:7000, message:"the micro blog will be repost has not send"};
                    cb(err, null, taskRecord); 
                }else{
                    cb(null, results[0], taskRecord); 
                }
            }
        });
    }
    
    //通过转发任务uri获取已发微博的id
    _self.getRepostTask = function(uri, cb){
        var uri = url.parse(uri);
        var id = uri.hash.substring(1);
        var repostRecord;
        var start = function(callback){
            _self.getTask(id, function(err, task){
                repostRecord = task;
                callback(err, task);
            });
        }
        
        var end = function(sentMicroBlog, callback){
            cb(null, sentMicroBlog.weibo_id, repostRecord);
        }
        
        async.waterfall([start, _self.getSentMicroBlog, end], function(err){
            cb(err, null, repostRecord);
        });
    }
    
    _self.reposted = function(record, weiboId, weiboUrl, repostedWeiboId, accountId, cb){
        var sql = "UPDATE repost_task SET repost_status = 1, repost_time = UNIX_TIMESTAMP() WHERE id = ?";
        dbWeibo.query(sql, [record.id], function(err, info){
            cb(err, info);  
        });
        
        var sql = "INSERT INTO reposted_micro_blog(article_id,stock_code,repost_time,weibo_id,weibo_url,reposted_weibo_id, account_id)"
                   + " VALUES(?, ?, ?, ?, ?, ?, ?)";
        var data = [0, record.stock_code, tool.timestamp(), weiboId, weiboUrl, repostedWeiboId, accountId];
        
        dbWeibo.query(sql, data, function(err, info){
            cb(err, info);
        });
        
    }
}
exports.db = new Db();
