var utilUrl = require("url");
var http = require("http");
var https = require("https");
var qs = require("querystring");
var fs = require("fs");
var path = require("path");
var get = function(url, data, callback){
	if(typeof data == 'function'){
		callback = data;
		data = null;
	}
	request(url, 'GET', data, null, callback);
}

var post = function(url, data, callback){
	if(typeof data == 'function'){
		callback = data;
		data = null;
	}
	request(url, 'POST', data, null, callback);
}

var upload = function(url, file, data, callback) {
    if(typeof data == 'function') {
        callback = data;
        data = null;
    }

    if(!path.existsSync(file)) {
        callback({ msg: "file " + file + " not exist" });
        return;
    }
    var filename = path.basename(file);
    var ext = path.extname(file);

    var mimes = {
        '.gif': 'image/gif',
        '.jpeg': 'image/jpeg',
        '.jpg': 'image/jpeg',
        '.png': 'image/png'
    };

    var boundary = 'boundary' + (new Date).getTime();
    var dashdash = '--';
    var crlf = '\r\n';

    /* Build RFC2388 string. */
    var builder = dashdash + boundary + crlf;

    for(var key in data) {
        /* Generate headers. key */
        builder += 'Content-Disposition: form-data; name="' + key + '"';
        builder += crlf;
        builder += crlf;
        /* Append form data. */
        builder += data[key];
        builder += crlf;

        /* Write boundary. */
        builder += dashdash + boundary + crlf;
    }

    builder += 'Content-Disposition: form-data; name="pic"';
    builder += '; filename="' + encodeURI(filename) + '"';
    builder += crlf;

    builder += 'Content-Type: ' + mimes[ext] ;
    builder += crlf;
    builder += crlf;

    var file_buffer = fs.readFileSync(file);
    var endstr = crlf + dashdash + boundary + dashdash + crlf;
    var builderLength = new Buffer(builder).length;                                                                                                                
    var size = builderLength + file_buffer.length + endstr.length;                                                                                               
    buffer = new Buffer(size);                                                                                                                                   
    var offset = 0;                                                                                                                                              
    buffer.write(builder);                                                                                                                                       
    offset += builderLength ;                                                                                                                                    
    file_buffer.copy(buffer, offset);                                                                                                                            
    offset += file_buffer.length;                                                                                                                                
    buffer.write(endstr, offset);  
    var headers = {};
    headers['Content-Type'] = 'multipart/form-data;boundary=' + boundary;
    request(url, 'POST', buffer, headers, callback);
}


var request = function(url, method, data, headers, callback) {
    if(typeof method == 'function') {
        callback = method;
        method = 'GET';
    }

    if(method == 'POST' && typeof data == 'function') {
        callback = data;
        data = '';
    }

    if(data && typeof data != 'string' && !Buffer.isBuffer(data)) {
        data = qs.stringify(data);
    }

    var url = utilUrl.parse(url);
    url.port = url.port || (url.protocol.match(/^https/) ? "443": "80");
    url.path = url.path || '/';
    if(data && method == 'GET') {
        if(url.path.indexOf('?') != -1) {
            url.path += '&' + data
        } else {
            url.path += '?' + data
        }
    }

    var options = {};
    options.host = url.hostname;
    options.port = url.port;
    options.path = url.path;
    options.method = method;

    if(!headers || typeof headers != 'object') {
        headers = {};
    }

    if(method == 'POST') {
        if(!headers['Content-Type']) {
            headers['Content-Type'] = 'application/x-www-form-urlencoded';
        }
        headers['Content-Length'] = data.length;
    }
    options.headers = headers;
    var reqMethod = url.protocol.match(/^https/) ? https.request : http.request;
    var req = reqMethod(options, function(res) {
        var body = '';
        res.on('data', function(chunk) {
            body += chunk;
        });

        res.on('end', function() {
            var result = null;
            try {
                result = JSON.parse(body);
            } catch(e) {
                result = body;
            }
            callback(null, result, res);
        });
    });

    req.setTimeout(30000, function() {
        callback({ msg: "timeout", status: 0 });
    });

    req.on('error', function(err) {
        callback({ msg: "http request error: " + err });
    });

    if(method == 'POST') {
        req.write(data);
    }
    req.end();
}

module.exports = {
	get:get,
	post:post,
    upload:upload,
	reqest:request
};


