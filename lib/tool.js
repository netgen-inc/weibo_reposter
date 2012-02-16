var tool = {
    getDateString : function(d, withTime){
        d = d || new Date();
        withTime = (typeof withTime === 'undefined') ? true : withTime;
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
    
    timestamp : function(d){
        d = d || new Date();
        return parseInt(d.getTime() / 1000);   
    }
};

exports.tool = tool;
