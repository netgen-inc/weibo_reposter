var settings = require(__dirname + '/etc/settings.json')
var de = require('devent').createDEvent('sender');
var queue = require('queuer');
var mysql = require("mysql");
var tool = require('./lib/tool').tool;
var mcli = mysql.createClient(settings.mysql.weibo);

var rq = queue.getQueue('http://'+settings.queue.host+':'+settings.queue.port+'/queue', settings.queue.repost);
de.on("open-url", function(microBlogId){
    console.log(micro_blog_id + " url opend");
    db.insertTask(microBlogId, 'a_stock', function(err, result){
        if(err){
            console.log(err);
        }else{
            var task = "mysql://172.16.39.117:3306/weibo?repost_task#" + result.insertId;
            rq.enqueue(task);
        }
    });
});

 var insertTask = function(microBlogId, stockCode, callback){
    var sql = "INSERT INTO repost_task(micro_blog_id, stock_code, in_time) VALUES(?, ?, ?)";
    mcli.query(sql, [microBlogId, stockCode, tool.timestamp()], callback);
}


setTimeout(function(){

}, 1000);