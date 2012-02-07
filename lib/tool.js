var tool = {
    getDataString : function(d, withTime){
        var d = d || new Date();
        var pad = function(x){
            if(x < 10){
                return '0' + x;
            }
            return x;
        }
        var date = [d.getFullYear(), pad(d.getMonth() + 1), pad(d.getDate())].join('-');
        if(withTime){
            var time = [pad(d.getHours()),  pad(d.getMinutes()), pad(d.getSeconds())].join(':')
            date += ' ' + time;
        }
        return date;
    },
    
    currentTimestamp : function(d){
        return parseInt(Date.now() / 1000);   
    }
};

exports.tool = tool;