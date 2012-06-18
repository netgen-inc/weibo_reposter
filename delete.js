var settings = require('./etc/settings'); 
var async = require('async');
var mysql = require('mysql');
var mcli = mysql.createClient(settings.weibo);
var weibo = require('weibo');
weibo.init('tsina', settings.weibo.appkey, settings.weibo.secret);

var weiboAccounts = {};
db.loadAccounts(function(err, accounts){
    if(err){
        console.log('!!!load account error!!!');   
        return;
    }
    weiboAccounts = accounts;
    console.log('access token loaded');
});

var del = function(task, callback){
    weibo.tapi.destroy(task, function(err, body, res){
        cosnole.log([err, body]);
        if(!err){
            var sql = "update reposted_micro_blog set deleted = 1 where weibo_id = ?"; 
            mcli.query(sql, [task.id], function(err){
                if(err){
                    console.log(err);   
                }    
            });  
        }
    });   
}

var q = async.queue(del, 5);
var sql = "SELECT weibo_id FROM reposted_micro_blog WHERE reposted_weibo_id IN (SELECT weibo_id FROM sent_micro_blog WHERE account_id = '3671')";
setTimeout(function(){
    mcli.query(sql, function(err, results){
        var user = weiboAccounts.sz900000;
        for(var i =0;i < results.length;i++){
            q.push({id:results[i].weibo_id,user:user});
        }
    });    
}, 1000);

setInterval(function(){
    q.length();
}, 1000);




