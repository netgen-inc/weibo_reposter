var mysql = require('mysql'); 
var url = require('url');
var async = require('async');

var Db = function(){
    var _self = this;
    var settings;
    var cli;
    _self.init = function(configs){
        settings = configs;
        cli = mysql.createClient(settings.mysql);
        cli.query('USE ' + settings.mysql.database);    
        cli.query('SET NAMES utf8');
    }   
    
    _self.loadAccounts = function(cb){
        var sql = "SELECT * FROM account";
        var expired = [], accounts = {};
        cli.query(sql, function(err, results){
            if(err){
                cb(err, null);
                return;
            }
            
            weiboAccounts = {};
            for(var i = 0; i < results.length;i++){
                var wa = results[i];
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
        cli.query(sql, [id], function(err, results){
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
    
    _self.getMicroBlog = function(articleStock, cb){
        var sql = "SELECT * FROM micro_blog WHERE article_id = ?";
        cli.query(sql, [articleStock.article_id], function(err, results){
            if(err){
                cb(err, results);   
            }else{
                if(results.length != 1){
                    cb({number:7002,message:'not found micro_blog by article_id ' + articleStock.article_id});
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
    
    _self.getSentMicroBlog = function(microBlog, cb){
        var sql = "SELECT * FROM sent_micro_blog WHERE micro_blog_id = ? AND deleted = 0";
        cli.query(sql, [microBlog.id], function(err, results){
            if(err){
                cb(err);   
            }else{
                if(results.length != 1){
                    cb({number:7003,message:'not found sent_micro_blog by micro_blog_id =' + microBlog.id});
                }else{
                    cb(null, results[0]);
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
            _self.getArticleStock(id, function(err, articleStock){
                if(!err){
                    repostRecord = articleStock;
                }
                callback(err, articleStock);
            });
            
        }
        
        var end = function(sentMicroBlog, callback){
            cb(null, sentMicroBlog.weibo_id, repostRecord);
        }
        
        async.waterfall([start, _self.getMicroBlog, _self.getSentMicroBlog, end], function(err){
            cb(err);
        });
    }
    
    _self.reposted = function(id, weiboId, weiboUrl, cb){
        var sql = "UPDATE article_stock SET weibo_id = ?, weibo_url = ?, repost_status = 1, repost_time = UNIX_TIMESTAMP() WHERE id = ?";
        cli.query(sql, [weiboId, weiboUrl, id], function(err, info){
            cb(err, info);  
        });
    }
}
exports.db = new Db();
