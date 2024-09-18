const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const port = process.env.port;
const host = process.env.host;
const dbname = process.env.dbname;
const username = process.env.u_name;
const password = process.env.password;

const sourcePool = mysql2.createPool({
  host: 'localhost', 
  port: 5200, 
  user: 'CkSS1408',
  password: '53151408',
  database: 'frontend',
  waitForConnections: true,
  connectionLimit: 10, 
  queueLimit: 0
});
const destinationPool = mysql2.createPool({
  host: 'cksgroup.my', 
  port: 3306, 
  user: 'cksroot',
  password: '4h]k53&[ugwN',
  database: 'cksgroup_intra',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

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
async function process_sales(tbldata,bizdate,sourceConnection,destinationConnection){
  try {      
    const [results] = await sourceConnection.query(`SELECT * FROM frontend.${tbldata} WHERE bizdate = '${bizdate}'`);    
    const insertPromises = results.map(row => {
      const columns = Object.keys(row).join(',');
      const values = Object.values(row).map(value => detectDateType(value) == null ? `null` : `'${detectDateType(value)}'`).join(',');
      return destinationConnection.query(`INSERT INTO cksgroup_intra.${tbldata} (${columns}) VALUES (${values})`);
    });    
    await Promise.all(insertPromises);    
    console.log('Data '+tbldata+' copied successfully');
    return true;
  } catch (err) {
    console.error('Error occurred:', err);
    return false;
  } 
}
function detectDateType(text){
  const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';
  if(moment(text,datetimeFormat,true).isValid()){
      return moment(text).format('YYYY-MM-DD HH:mm:ss');
  }else if(moment(text,dateFormat,true).isValid()){
      return moment(text).format('YYYY-MM-DD');
      }else{
          if(text=='Invalid Date'){text='0000-00-00 00:00:00'}
          if (typeof text === 'string') {text = text.replace(/\\/g, "");text = text.replace(/'/g, "\\'");}            
          return text;
      }
}

async function get_lastsync(){
    const tmpstamp = new Date();console.log('Populating get_lastsync : '+tmpstamp);
    let connPanda,connWeb
    isget_lastsync=0;
    try {        
        connWeb = await dbpool2.getConnection();        
        let qry="SELECT `table_name`,CAST(DATE(last_sync) AS CHAR)BizDate FROM db_sync WHERE ACTIVE=1 AND ids='4' AND DATE(last_sync) <DATE(NOW())";
        const lastsync = await connWeb.query(qry);         
        if(await lastsync[0].length>0){
          for (let i = 0; i < lastsync[0].length; i++) {                        
            const table_name =lastsync[0][i]['table_name'];            
            const array_table_name=table_name.split(",");            
            const BizDate =lastsync[0][i]['BizDate']
            let sourceConnection;
            let destinationConnection;
            try {
              sourceConnection = await sourcePool.getConnection();
              destinationConnection = await destinationPool.getConnection();              
              await sourceConnection.beginTransaction();
              await destinationConnection.beginTransaction();              
              for (let ii = 0; ii < array_table_name.length; ii++) {
                  console.log('populating : '+array_table_name[ii]+' '+BizDate);
                  const _table_name=array_table_name[ii];
                  await destinationConnection.query(`DELETE FROM cksgroup_intra.${_table_name} WHERE BizDate='${BizDate}'`); 
                  let isDone= await process_sales(_table_name,BizDate,sourceConnection,destinationConnection);
                  if(!isDone){
                    throw new Error('Err Copying Data');
                  }
              }e
              const Newdate = new Date(BizDate);
              Newdate.setDate(Newdate.getDate() + 1);
              let newDate = Newdate.toISOString().split('T')[0];
              newDate=newDate+' 00:00:00';
              await destinationConnection.query(`UPDATE cksgroup_intra.db_sync SET last_sync='${newDate}' WHERE ids='4'`);
              console.log('Sales Update day to :  '+newDate)
              await sourceConnection.commit();
              await destinationConnection.commit();
            } catch (err) {
              console.error('Error occurred:', err);
              if (sourceConnection) await sourceConnection.rollback();
              if (destinationConnection) await destinationConnection.rollback();
              return false;
            } finally {    
              if (sourceConnection) sourceConnection.release();
              if (destinationConnection) destinationConnection.release();                                          
            }
          }
        }        
    } catch (error) {                
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {      
      if (connWeb) {connWeb.release();}
    }
    isget_lastsync=1; 
    console.log('Populating get_lastsync Done');     
}
console.clear();
get_lastsync();
// const CronJob = require('cron').CronJob;
// const jb1 = new CronJob("0 * * * *", function() {if(isget_lastsync==1){isget_lastsync=0;get_lastsync();}});	
// jb1.start();

