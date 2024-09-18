const mysql = require('mysql2/promise');
const moment = require('moment');
const sourcePool = mysql.createPool({
  host: 'localhost', 
  port: 5200, 
  user: 'CkSS1408',
  password: '53151408',
  database: 'frontend',
  waitForConnections: true,
  connectionLimit: 10, 
  queueLimit: 0
});
const destinationPool = mysql.createPool({
  host: 'cksgroup.my', 
  port: 3306, 
  user: 'cksroot',
  password: '4h]k53&[ugwN',
  database: 'cksgroup_intra',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});
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
const copyData = async () => {
  let sourceConnection;
  let destinationConnection;

  try {    
    sourceConnection = await sourcePool.getConnection();
    destinationConnection = await destinationPool.getConnection();    
    await sourceConnection.beginTransaction();
    await destinationConnection.beginTransaction();    
    const [results] = await sourceConnection.query(`SELECT * FROM frontend.poschild WHERE bizdate = '2024-07-03'`);    
    const insertPromises = results.map(row => {
      const columns = Object.keys(row).join(',');
      const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
      return destinationConnection.query(`INSERT INTO cksgroup_intra.poschild (${columns}) VALUES (${values})`);
    });    
    await Promise.all(insertPromises);    
    await sourceConnection.commit();
    await destinationConnection.commit();
    console.log('Data copied successfully');
  } catch (err) {
    console.error('Error occurred:', err);
    if (sourceConnection) await sourceConnection.rollback();
    if (destinationConnection) await destinationConnection.rollback();
  } finally {    
    if (sourceConnection) sourceConnection.release();
    if (destinationConnection) destinationConnection.release();
  }
  process.exit();
};
console.clear();
console.log('Starting copying POSCHILD');
copyData();
