const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');

const port = process.env.port;
const host = process.env.host;
const dbname = process.env.dbname;
const username = process.env.u_name;
const password = process.env.password;
let isget_lastsync=1

const dbpool= mysql2.createPool({
	host: host,port: port,user: username,password: password,database:dbname,
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
const dbpool2= mysql2.createPool({
	host: 'cksgroup.my',port: '3306',user: 'cksroot',password: '4h]k53&[ugwN',database:'cksgroup_intra',
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
async function get_table_schema(table_name,database_name,conn){try{let field_name="",qry="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+database_name+"' AND TABLE_NAME='"+table_name+"'",tbldata=await conn.query(qry);for(let i=0;i<tbldata[0].length;i++)field_name+=tbldata[0][i]['COLUMN_NAME']+",";return field_name=field_name.slice(0,-1),field_name}catch(error){return console.log(error),!1}}

async function source_to_destination(fields,table_name,LastSync,database_name,syncfield,conn,connWeb){
  try {
    const tmpstamp = new Date();
    let lastsync=detectDateType(tmpstamp)
    let qry="select "+fields+" from "+database_name+"."+table_name;        
    let tbldata = await conn.query(qry);    
    await connWeb.beginTransaction();
    for (const obj of tbldata[0]) {
      let tmpFld='',tmpDat=''
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          tmpFld+=key+','
          let tmp = detectDateType(obj[key])
          if(tmp=='Invalid Date'){tmp='0000-00-00 00:00:00'}
          if (typeof tmp === 'string') {
            tmp = tmp.replace(/'/g, "\\'");
          }
          if(tmp==null){tmpDat+=""+tmp+","}else{tmpDat+="'"+tmp+"',"}
        }
      }
      tmpDat=tmpDat.slice(0, -1)
      tmpFld=tmpFld.slice(0, -1)
      qry ="replace into cksgroup_intra."+table_name+"("+tmpFld+")value"
      qry = qry + "("+tmpDat+")"      
      await connWeb.query(qry); 
      qry="update db_sync SET last_sync='"+lastsync+"' where database_name='"+database_name+"' and table_name='"+table_name+"'"      
      await connWeb.query(qry);
      await connWeb.commit();
    }    
    return true
  } catch (error) {
    console.log(error)
    if (connWeb) {connWeb.rollback();}    
    return false
  }
}
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}

async function get_lastsync(){
    const tmpstamp = new Date();console.log('Populating PO BY Supp/Outlet : '+tmpstamp);
    let connWeb
    isget_lastsync=0;
    try {        
        connWeb = await dbpool2.getConnection();        
        let qry="";
        const currentYear = new Date().getFullYear();
        const loc_list = await connWeb.query("SELECT `code` from location");         
        const tmpMth='04';
        let tmp_date_range='"'+currentYear+'-'+tmpMth+'-01" and "'+currentYear+'-'+tmpMth+'-07"'
        for (let y = 0; y < loc_list[0].length; y++) {                                 
            let tmp_qry="SELECT location,Scode,SName,COUNT(*)cnt FROM pomain  WHERE location='"+loc_list[0][y]['code']+"' AND podate between "+tmp_date_range+" GROUP BY Scode"
            const polist = await connWeb.query(tmp_qry);
            if(await polist[0].length>0){
                for (let z = 0; z < polist[0].length; z++) {
                    let tmplocation=polist[0][z]['location'],tmpScode=polist[0][z]['Scode'],tmpSName=polist[0][z]['SName'],tmpcnt=polist[0][z]['cnt']
                    await connWeb.query('replace supp_po_bymonthQ(scode,sname,tmp_year,tmp_month,poweek,outlet,pocount)value("'+tmpScode+'","'+tmpSName+'","'+currentYear+'","'+tmpMth+'",1,"'+tmplocation+'",'+tmpcnt+')'); 
                }                    
            }
        }            
        let tmp_date_range2='"'+currentYear+'-'+tmpMth+'-08" and "'+currentYear+'-'+tmpMth+'-14"'
        for (let y = 0; y < loc_list[0].length; y++) {                                 
            let tmp_qry="SELECT location,Scode,SName,COUNT(*)cnt FROM pomain  WHERE location='"+loc_list[0][y]['code']+"' AND podate between "+tmp_date_range2+" GROUP BY Scode"
            const polist = await connWeb.query(tmp_qry);
            if(await polist[0].length>0){
                for (let z = 0; z < polist[0].length; z++) {
                    let tmplocation=polist[0][z]['location'],tmpScode=polist[0][z]['Scode'],tmpSName=polist[0][z]['SName'],tmpcnt=polist[0][z]['cnt']
                    await connWeb.query('replace supp_po_bymonthQ(scode,sname,tmp_year,tmp_month,poweek,outlet,pocount)value("'+tmpScode+'","'+tmpSName+'","'+currentYear+'","'+tmpMth+'",2,"'+tmplocation+'",'+tmpcnt+')'); 
                }                    
            }
        }            
        let tmp_date_range3='"'+currentYear+'-'+tmpMth+'-15" and "'+currentYear+'-'+tmpMth+'-21"'
        for (let y = 0; y < loc_list[0].length; y++) {                                 
            let tmp_qry="SELECT location,Scode,SName,COUNT(*)cnt FROM pomain  WHERE location='"+loc_list[0][y]['code']+"' AND podate between "+tmp_date_range3+" GROUP BY Scode"
            const polist = await connWeb.query(tmp_qry);
            if(await polist[0].length>0){
                for (let z = 0; z < polist[0].length; z++) {
                    let tmplocation=polist[0][z]['location'],tmpScode=polist[0][z]['Scode'],tmpSName=polist[0][z]['SName'],tmpcnt=polist[0][z]['cnt']
                    await connWeb.query('replace supp_po_bymonthQ(scode,sname,tmp_year,tmp_month,poweek,outlet,pocount)value("'+tmpScode+'","'+tmpSName+'","'+currentYear+'","'+tmpMth+'",3,"'+tmplocation+'",'+tmpcnt+')'); 
                }                    
            }
        }            
        let tmp_date_range4='"'+currentYear+'-'+tmpMth+'-22" and "'+currentYear+'-'+tmpMth+'-31"'
        for (let y = 0; y < loc_list[0].length; y++) {                                 
            let tmp_qry="SELECT location,Scode,SName,COUNT(*)cnt FROM pomain  WHERE location='"+loc_list[0][y]['code']+"' AND podate between "+tmp_date_range4+" GROUP BY Scode"
            const polist = await connWeb.query(tmp_qry);
            if(await polist[0].length>0){
                for (let z = 0; z < polist[0].length; z++) {
                    let tmplocation=polist[0][z]['location'],tmpScode=polist[0][z]['Scode'],tmpSName=polist[0][z]['SName'],tmpcnt=polist[0][z]['cnt']
                    await connWeb.query('replace supp_po_bymonthQ(scode,sname,tmp_year,tmp_month,poweek,outlet,pocount)value("'+tmpScode+'","'+tmpSName+'","'+currentYear+'","'+tmpMth+'",4,"'+tmplocation+'",'+tmpcnt+')'); 
                }                    
            }
        }      
    } catch (error) {        
        console.log(error)        
        if (connWeb) {connWeb.rollback();}        
    } finally {      
      if (connWeb) {connWeb.release();}
    }
    isget_lastsync=1; 
    console.log('Populating PO BY Supp/Outlet list Done');
    process.exit();
}
console.clear();
get_lastsync();