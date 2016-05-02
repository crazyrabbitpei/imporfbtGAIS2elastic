'use strict'
var elasticsearch = require('elasticsearch');
var fs = require('graceful-fs');
var setting = JSON.parse(fs.readFileSync("config/setting"));

var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');

var LineByLineReader = require('line-by-line');
var S = require('string');
const exec = require('child_process').exec;

var HashMap = require('hashmap');

const EventEmitter = require('events');
class MyEmitter extends EventEmitter {};
const import_bot = new MyEmitter();

var client;
var tag;
var import_again=0;
var nums = 0;
var import_record_nums=0;
var count_importfile=0;
var import_index=0;

var now_imports=0;
var file_import_end=0;
var fname;
var file_map = new HashMap();


var importedList = new HashMap();
var ip = setting['db_ip'];
var port = setting['db_port'];
var datafilename = setting['filename'];

if(process.argv[2]=="data"){
    var dbname = setting['fb_dbname'];
    var table = setting['fb_table'];
    var column = setting['fb_column'];
    var dataDir = setting['dataDir'];

    readImportedList("/home/crazyrabbit/import2elastic/logs/1_total_list.list",function(){
        connect2DB(ip,port,function(stat){
            start();
        });
    });

}
else if(process.argv[2]=='group'){
    var dbname = setting['fbgroups_dbname'];
    var table = setting['fbgroups_table'];
    var column = setting['fbgroups_column'];
    var dataDir = setting['groupsDir'];

    connect2DB(ip,port,function(stat){
        readlist(dataDir,datafilename,function(total_column){
            gais2json(total_column,dataDir,function(result){
                console.log("["+result+"] done");
            });
        });
    });

}
else{
    console.log("Type error, input 'data' or 'group'");
    process.exit();

}
/*
import_bot.on("afile_done",function(){
    if(import_index==nums){
        console.log("All list imported.");
    }
    else{
        var promise1 = new Promise(function(resolve,reject){
            console.log("["+fname[import_index]+"] start");
            gais2json(total_column,fname[import_index],function(result){
                resolve(result);   
            });

        });
        promise1.then(function(value){
            console.log("["+value+"] done");
            fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                if(err){
                    console.log(err);
                }
            });
            if(import_index==nums){
                console.log("All list imported.");
            }
        }).catch(function(error){
            fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                if(err){
                    //console.log("write log false:"+error);
                }
            });
        });

        import_index++;
        count_importfile++;
    }
});
*/
function start(){
    var date = dateFormat(new Date(), "yyyymmdd");
    readlist(dataDir,datafilename,function(total_column){
        nums = fname.length-1;
        var i=0;
        for(i=0;i<nums;i++,count_importfile++){
            if(typeof importedList.get(fname[i])!=="undefined"){
                console.log(fname[i]+" imported");
                continue;
            }
            else{
                break;
            }
        }
        var promise1 = new Promise(function(resolve,reject){
            console.log("["+fname[import_index]+"] start");
            gais2json(total_column,fname[import_index],function(result){
                resolve(result);   
            });
            import_index++;

        });
        promise1.then(function(value){
            console.log("["+value+"] done");
            fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                if(err){
                    console.log(err);
                }
            });
            if(import_index==nums){
                console.log("All list imported.");
            }
        }).catch(function(error){
            fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                if(err){
                    //console.log("write log false:"+error);
                }
            });
        });

        count_importfile++;
        /*
        if(count_importfile==nums){
            console.log("All list imported.");
            return;
        }
        */
        /*
        tag = setInterval(function(){
            var promise = new Promise(function(resolve,reject){
                gais2json(total_column,fname[i],function(result){
                    resolve(result);   
                });

            });
            promise.then(function(value){
                console.log("["+value+"] done");
                fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }
                });

                count_importfile++;
                if(count_importfile==nums){
                    console.log("All list imported.");
                }
            }).catch(function(error){
                console.log('promise err:'error);
                fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }
                });
                clearInterval(tag);
            });

            i++;
            if(i>nums){
                console.log("["+i+"]>["+nums+"] Stop interval and waitting....");
                clearInterval(tag);
            }
        },60*1000);
        */
    });

}

function anotherimport(total_column){
    if(import_index==nums){
        console.log("All list imported.");
    }
    else{
        var promise1 = new Promise(function(resolve,reject){
            console.log("0.["+fname[import_index]+"] start");
            gais2json(total_column,fname[import_index],function(result){
                resolve(result);   
            });
            import_index++;

        });
        promise1.then(function(value){
            console.log("["+value+"] done");
            fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                if(err){
                    console.log(err);
                }
            });
            if(import_index==nums){
                console.log("All list imported.");
            }
        }).catch(function(error){
            console.log("another error:"+error);
            fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                if(err){
                    //console.log("write log false:"+error);
                }
            });
        });
        count_importfile++;
    }
    
}

/*not yet*/
var job = new CronJob({
    cronTime:"50 59 23 * * *",
    onTick:function(){
        var date = dateFormat(new Date(), "yyyymmdd");
        readlist(dataDir,date,function(total_column){
            for(i=0;i<fname.length-1;i++){

                //console.log("["+i+"] "+fname[i]);
                gais2json(total_column,fname[i],function(result){
                    //console.log("result:"+result); 
                });
            } 
        });
    },
    start:false,
    timeZone:'Asia/Taipei'
});

function connect2DB(dbip,dbport,fin){
    client = new elasticsearch.Client({
        host:dbip+':'+dbport,
    });
    fin("connect ok");
}

function readlist(dir,filename,fin){
//get all board, read in array, recording file name (use exec find . -name 2016* | grep -c ""   doesn't need _stop file)
    //console.log(`find `+dir+` -name `+filename);
    //return;
    //const child = exec(`find `+dir+` -name `+filename,(error,stdout,stderr) => {
    
    if(process.argv[2]=="data"){
        fs.readFile(dir,'utf8',(err,data) => {
            if(err){
                console.log('readlist error:'+err);
            }
            else{
                //console.log('data:'+data);
                fname = data.split("\n");

                var total_column = [];
                var i;
                
                for(i=0;i<fname.length-1;i++){
                    //console.log("["+i+"]"+fname[i]);
                    file_map.set(fname[i],0);
                }
                //read gais column's name
                var column_name = Object.keys(column);
                column_name.forEach(function(cname){
                    var items = Object.keys(column[cname]);
                    items.forEach(function(item) {
                        var value = column[cname][item];
                        //console.log(cname+': '+item+' = '+value);
                        total_column.push(item);

                    });
                });
                /*
                   for(i=0;i<total_column.length;i++){
                   console.log(total_column[i]+":"+total_column[i].length);
                   }
                   */
                fin(total_column);

            }
        });

    }
    else if(process.argv[2]=="group"){
        var total_column = [];
        var i;
        //read gais column's name
        var column_name = Object.keys(column);
        column_name.forEach(function(cname){
            var items = Object.keys(column[cname]);
            items.forEach(function(item) {
                var value = column[cname][item];
                //console.log(cname+': '+item+' = '+value);
                total_column.push(item);

            });
        });
        fin("",total_column);
    }
}

function gais2json(cname,dir,fin){
    //read file 2016...
    if(process.argv[2]=="data"){
        var full_dir = "/home/crazyrabbit/GraphBot_beta/fb_data/"+dir;
        readGaisdata(cname,full_dir,dir,function(stat){
            console.log("gais2json:"+stat);
            fin(stat);
        });   
    }
    else if(process.argv[2]=="group"){
        var full_dir = dir;
        readGaisgroups(cname,full_dir,function(stat){
            fin(stat);
        });   
    }
}

function readImportedList(filename,fin){
    var i;
    var body_flag=0;
    var content = [];
    var body="";

    var options = {
        skipEmptyLines:true
    }
    var lr = new LineByLineReader(filename,options);
    lr.on('error', function (err) {
        console.log("["+filename+"]error:"+err);
        fs.appendFile("./logs/err_filename",filename+"\n",function(err){
            
        });
    });
    lr.on('line', function (line) {
        //var parts = line.split("/");
        //var newdir = "./"+parts[parts.length-2]+"/"+parts[parts.length-1];
        importedList.set(line,"1");
    });
    lr.on('end',function(){
        fin("ok");
    });
}
function readGaisgroups(cname,filename,fin){
    var i;
    var body_flag=0,description_flag=0;
    var content = [];
    var options = {
        skipEmptyLines:true
    }
    var lr = new LineByLineReader(filename,options);
    lr.on('error', function (err) {
        console.log("error:"+err);
    });
    lr.on('line', function (line) {
        //cut and get column info
        for(i=0;i<cname.length;i++){
            //console.log("cname["+i+"]:"+cname[i]);
            if(line=="@"){
                continue;
            }
            if(cname[i]=="@id"&&line.indexOf("@id")!=-1){
                body_flag=0;
                description_flag=0;
                //conbert to json
                    var record = JSON.stringify({
                        id:content[0],
                        name:content[1],
                        location:content[2],
                        category:content[3],
                        likes:content[4],
                        talking_about_count:content[5],
                        were_here_count:content[6]
                    });
                    var tname = S(filename).right(8).s;
                    tname = S(tname).left(6).s;//use YYYYMM ex:201602 for table's name

                    import_record_nums++;
                    //fs.appendFile("./test.record",record+"\n",function(){});
                    
                    setTimeout(function(){
                        import2db(dbname,tname,record);
                    },500);
                    
                    content = [];
                if(S(line).left(cname[i].length).s==cname[i]){
                    //console.log("["+i+"]"+cname[i]+":"+S(line).right(line.length-cname[i].length-1).s);
                    content.push(S(line).right(line.length-cname[i].length-1).s);
                    break;
                }

            }
            if(S(line).left(cname[i].length).s==cname[i]){
                //console.log("["+i+"]"+cname[i]+":"+S(line).right(line.length-cname[i].length-1).s);
                content.push(S(line).right(line.length-cname[i].length-1).s);
                break;
            }
        }

    });
    lr.on('end',function(){
        var record = JSON.stringify({
            id:content[0],
            name:content[1],
            location:content[2],
            category:content[3],
            likes:content[4],
            talking_about_count:content[5],
            were_here_count:content[6]
        });
        var tname = S(filename).right(8).s;
        tname = S(tname).left(6).s;

        import_record_nums++;
        //fs.appendFile("./test.record",record+"\n",function(){});
        setTimeout(function(){
            import2db(dbname,tname,record);
        },500);
        
        content = [];
        //console.log("read ["+filename+"] done");
        fin(filename);
    });
}
function readGaisdata(cname,filename,real_filename,fin){
    var i;
    var body_flag=0,description_flag=0;
    var content = [];
    var body="";
    var description="";
    var options = {
        skipEmptyLines:true
    }
    var lr = new LineByLineReader(filename,options);
    lr.on('error', function (err) {
        console.log("error:"+err);
    });
    lr.on('line', function (line) {
        //cut and get column info
        for(i=0;i<cname.length;i++){
            //console.log("cname["+i+"]:"+cname[i]);
            if(line=="@"){
                body_flag=0;
                description_flag=0;
                continue;
            }
            if(cname[i]=="@title"&&line.indexOf("@title")!=-1){
                body_flag=0;
                description_flag=0;
                //conbert to json
                if(body!=""){
                    var record = JSON.stringify({
                        title:content[0],
                        source:content[1],
                        url:content[2],
                        time:content[3],
                        description:description,
                        body:body
                    });
                    body="";
                    description="";
                    var tname = S(filename).right(8).s;
                    tname = S(tname).left(6).s;//use YYYYMM ex:201602 for table's name

                    import_record_nums++;
                    //fs.appendFile("./test.record",record+"\n",function(){});
                    var random_time = parseInt(Math.floor(Math.random()*(60)+1));
                    //console.log('random_time:'+random_time);
                    setTimeout(function(){
                        now_imports++;
                        import2db(cname,real_filename,dbname,tname,record,fin);
                    },random_time*100);
                    
                    if(now_imports>100){
                        lr.pause();
                        setTimeout(function(){
                            lr.resume();
                        },10*1000);
                    }
                    content = [];
                }
                if(S(line).left(cname[i].length).s==cname[i]){
                    //console.log("["+i+"]"+cname[i]+":"+S(line).right(line.length-cname[i].length-1).s);
                    content.push(S(line).right(line.length-cname[i].length-1).s);
                    break;
                }

            }
            if(S(line).left(cname[i].length).s=="@body"&&cname[i]=="@body"){
                body_flag=1;
                line = S(line).right(line.length-cname[i].length-1).s;
                body += line;
                //console.log("["+i+"]"+cname[i]+":");
                
                break;
            }
            else if(S(line).left(cname[i].length).s=="@description"&&cname[i]=="@description"){
                body_flag=0;
                description_flag=1;
                line = S(line).right(line.length-cname[i].length-1).s;
                description += line;
                //console.log("["+i+"]"+cname[i]+":");
                break;
            }
            else if(S(line).left(cname[i].length).s==cname[i]){
                //console.log("["+i+"]"+cname[i]+":"+S(line).right(line.length-cname[i].length-1).s);
                content.push(S(line).right(line.length-cname[i].length-1).s);
                break;
            }
            
            else if(body_flag==1&&line.indexOf("@description")==-1){
                if(body==""){
                    body += line;
                }
                else{
                    body += " "+line;
                }

                //console.log(line);
                break;
            }
            else if(description_flag==1){
                if(description==""){
                    description += line;
                }
                else{
                    description += " "+line;
                }
                //console.log(line);
                break;
            }
            
        }

    });
    lr.on('end',function(){

        var record = JSON.stringify({
            title:content[0],
            source:content[1],
            url:content[2],
            time:content[3],
            description:description,
            body:body
        });
        var tname = S(filename).right(8).s;
        tname = S(tname).left(6).s;

        import_record_nums++;
        //fs.appendFile("./test.record",record+"\n",function(){});
        var random_time = parseInt(Math.floor(Math.random()*(60)+1));
        //console.log('random_time:'+random_time);
        setTimeout(function(){
            file_import_end=1;
            now_imports++;
            import2db(cname,real_filename,dbname,tname,record,fin);
        },random_time*100);
        
        if(now_imports>100){
            lr.pause();
            setTimeout(function(){
                lr.resume();
            },10*1000);
        }
        content = [];
        //console.log("read ["+filename+"] done");
        //fin(filename);
    });
}
function import2db(cname,filename,dname,tname,content,fin){

    var date = dateFormat(new Date(), "yyyymmdd");
    try{
        var for_id = JSON.parse(content);
        if(process.argv[2]=="data"){
            var url = for_id['url'];
        }
        else if(process.argv[2]=="group"){
            var url = for_id['id'];
        }
        client.create({
            index:dbname,
            type:table,
            id:url,
            body:content
        },function(error,response){
            if(file_map.get(filename)!=0){
                now_imports--;
                return;
            }
            if(!error&&!response.error){
                now_imports--;
                console.log("0.["+filename+"]now_imports:"+now_imports);
                //console.log("file_import_end:"+file_import_end);
                //console.log("file_map.get("+filename+")"+file_map.get(filename));
                if(file_import_end==1&&now_imports==0&&file_map.get(filename)==0){
                    file_import_end=0;
                    file_map.set(filename,1);
                    //import_bot.emit('afile_done');
                    var date = dateFormat(new Date(), "yyyymmdd");
                    console.log("1.["+filename+"] done");
                    fs.appendFile("logs/import_"+date+".list",filename+"\n",function(err){
                        if(err){
                            console.log(err);
                        }
                    });
                    if(import_index==nums){
                        console.log("All list imported.");
                    }
                    anotherimport(cname);
                    //fin(filename);
                }
                /*
                   var result = JSON.stringify(response);
                   fs.appendFile("logs/success.log",result+"\n",function(err){
                   if(err){
                //console.log("write log false:"+error);
                }
                });
                */
            }
            else{
                //console.log("write log false");
                var temp="",code="";
                if(typeof response!=="undefined"){
                    temp = JSON.stringify(response.error);
                    code = response.status;
                    if(code!="409"&&code!="200"&&code!="201"){
                        fs.appendFile("logs/err_"+date+".content",content+"\n",function(err){
                            if(err){
                                //console.log("write log false:"+error);
                            }
                        });

                    }
                }
                else{
                    temp = error;
                    code = "503";
                }

                if(code=="503"){
                    fs.appendFile("logs/restart_"+date+".log","code:"+code+"\n"+temp+"\ncontent:"+content+"--\n",function(err){
                        if(err){
                            //console.log("write log false:"+error);
                        }

                    });
                    //console.log("Sleep for 1 munutes...");
                    var random_time = parseInt(Math.floor(Math.random()*(60)+1));
                    console.log("Reimport after "+random_time+" secs");
                    setTimeout(function(){
                        import2db(cname,filename,dname,tname,content,fin);
                    },random_time*1000);
                }
                else if(code!="409"){
                    now_imports--;
                    console.log("1.["+filename+"]now_imports:"+now_imports);
                    //console.log("file_import_end:"+file_import_end);
                    //console.log("file_map.get("+filename+")"+file_map.get(filename));
                    if(file_import_end==1&&now_imports==0&&file_map.get(filename)==0){
                        file_import_end=0;
                        file_map.set(filename,1);
                        //import_bot.emit('afile_done');
                        var date = dateFormat(new Date(), "yyyymmdd");
                        console.log("2.["+filename+"] done");
                        fs.appendFile("logs/import_"+date+".list",filename+"\n",function(err){
                            if(err){
                                console.log(err);
                            }
                        });
                        if(import_index==nums){
                            console.log("All list imported.");
                        }
                        anotherimport(cname);
                        //fin(filename);
                    }
                    fs.appendFile("logs/err_"+date+".log","code:"+code+"\n"+temp+"\n--\n",function(err){
                        if(err){
                            //console.log("write log false:"+error);
                        }

                    });
                }
                else{
                    now_imports--;
                    console.log("2.["+filename+"]now_imports:"+now_imports);
                    //console.log("file_import_end:"+file_import_end);
                    //console.log("file_map.get("+filename+")"+file_map.get(filename));
                    if(file_import_end==1&&now_imports==0&&file_map.get(filename)==0){

                        file_import_end=0;
                        file_map.set(filename,1);
                        //import_bot.emit('afile_done');
                        var date = dateFormat(new Date(), "yyyymmdd");
                        console.log("3.["+filename+"] done");
                        fs.appendFile("logs/import_"+date+".list",filename+"\n",function(err){
                            if(err){
                                console.log(err);
                            }
                        });
                        if(import_index==nums){
                            console.log("All list imported.");
                        }
                        anotherimport(cname);
                        //fin(filename);
                    }
                }
            }
        });
    }
    catch(e){
        console.log('import error:'+e+"\n"+'filename:'+filename+'\ncontent:'+content);
    }
}
