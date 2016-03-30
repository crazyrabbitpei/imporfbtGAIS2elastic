var elasticsearch = require('elasticsearch');
var fs = require('graceful-fs');
var setting = JSON.parse(fs.readFileSync("config/setting"));

var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');

var LineByLineReader = require('line-by-line');
var S = require('string');
const exec = require('child_process').exec;


var ip = setting['db_ip'];
var port = setting['db_port'];
if(process.argv[2]=="data"){
    var dbname = setting['fb_dbname'];
    var table = setting['fb_table'];
    var column = setting['fb_column'];
    var dataDir = setting['dataDir'];
}
else if(process.argv[2]=='group'){
    var dbname = setting['fbgroups_dbname'];
    var table = setting['fbgroups_table'];
    var column = setting['fbgroups_column'];
    var dataDir = setting['groupsDir'];
}
else{
    console.log("Type error, input 'data' or 'group'");
    process.exit();
}
var import_record_nums=0;

var client;
var tag;

connect2DB(ip,port,function(stat){
    var scolumn = process.argv[3];
    var pattern = process.argv[4];
    console.log(scolumn+":"+pattern);
    var fields = "";
    search(dbname,table,scolumn,pattern,fields);
});


function connect2DB(dbip,dbport,fin){
    client = new elasticsearch.Client({
        host:dbip+':'+dbport,
    });
    fin("connect ok");
}

function search(dname,tname,scolumn,pattern,fields){
    client.search({
        index:dbname,
        type:table,
        q:scolumn+":"+pattern
    },function(err,response){
        console.log(response.hits.total);
    });
}
