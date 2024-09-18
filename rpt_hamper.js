const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const { format, startOfYear, startOfMonth, endOfMonth, addMonths } = require('date-fns');

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
function getFirstDecimalPointValue(num) {
    const numStr = num.toString();
    const [integerPart, decimalPart] = numStr.split('.');
    if (decimalPart && decimalPart.length > 0) {
        return parseInt(decimalPart[0], 10);
    }
    return 0;
}
function escapeSingleQuotes(str) {return str.replace(/'/g, "\\'");}
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}
async function process_data(code,barcode,location){
    const tmpstamp = new Date();console.log('Populating Hamper : '+location);
    let queries = [];
    let connPanda,connWeb,qry=''
    const startDate = startOfYear(new Date()); 
    const endDate = startOfMonth(new Date());    
    // const finalQuery = await generateQueries(startDate, endDate, location, barcode);
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        let yrs='2024'
        let query ="SELECT LEFT(bizdate, 4)bizdateyear,SUM(RM30)RM30,SUM(RM50)RM50,SUM(RM100)RM100,SUM(RM150)RM150,SUM(RM200)RM200 FROM(";
        query +=" SELECT bizdate, location, MAX(RM30) RM30, MAX(RM50) RM50, MAX(RM100) RM100, MAX(RM150) RM150, MAX(RM200) RM200 FROM (";
        query +=" SELECT LEFT(bizdate, 7) bizdate, location,SUM(costmargin)costmargin,";
        query +=" IF(LEFT(description, 11) = 'HAMPER_RM30', SUM(qty), 0) RM30,";
        query +=" IF(LEFT(description, 11) = 'HAMPER_RM50', SUM(qty), 0) RM50,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM100', SUM(qty), 0) RM100,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM150', SUM(qty), 0) RM150,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM200', SUM(qty), 0) RM200"
        query +=" FROM frontend.poschild"
        query +=" WHERE bizdate BETWEEN '"+yrs+"-01-01' AND '"+yrs+"-08-31'";
        query +=" AND Location IN ("+location+")";
        query +=" AND barcode IN ("+barcode+")";
        query +=" AND price IN(30,50,100,150,200)";
        query +=" GROUP BY price,barcode,location,LEFT(bizdate,7)) A";  
        query +=" GROUP BY LEFT(bizdate,7),location)a ORDER BY bizdate";
        let query2 ="SELECT LEFT(bizdate, 7)bizdate,location,SUM(RM30)RM30,SUM(RM50)RM50,SUM(RM100)RM100,SUM(RM150)RM150,SUM(RM200)RM200 FROM(";
        query2 +=" SELECT bizdate, location, MAX(RM30) RM30, MAX(RM50) RM50, MAX(RM100) RM100, MAX(RM150) RM150, MAX(RM200) RM200 FROM (";
        query2 +=" SELECT LEFT(bizdate, 7) bizdate, location,SUM(costmargin)costmargin,";
        query2 +=" IF(LEFT(description, 11) = 'HAMPER_RM30', SUM(qty), 0) RM30,";
        query2 +=" IF(LEFT(description, 11) = 'HAMPER_RM50', SUM(qty), 0) RM50,"
        query2 +=" IF(LEFT(description, 12) = 'HAMPER_RM100', SUM(qty), 0) RM100,"
        query2 +=" IF(LEFT(description, 12) = 'HAMPER_RM150', SUM(qty), 0) RM150,"
        query2 +=" IF(LEFT(description, 12) = 'HAMPER_RM200', SUM(qty), 0) RM200"
        query2 +=" FROM frontend.poschild"
        query2 +=" WHERE bizdate BETWEEN '"+yrs+"-01-01' AND '"+yrs+"-08-31'";
        query2 +=" AND Location IN ("+location+")";
        query2 +=" AND barcode IN ("+barcode+")";
        query2 +=" AND price IN(30,50,100,150,200)";
        query2 +=" GROUP BY price,barcode,location,LEFT(bizdate,7)) A";  
        query2 +=" GROUP BY LEFT(bizdate,7),location)a GROUP BY LEFT(bizdate,7),location ORDER BY bizdate";        
        // console.log(query);process.exit();
        let tmp_tables2 = await connPanda.query(query);         
        let tmp_tables3 = await connPanda.query(query2);
        const jsonString = JSON.stringify(tmp_tables3[0]);
        const escapedJsonString = escapeSingleQuotes(jsonString);          
        let series=[],categories='';
        if(await tmp_tables2[0].length>0){
            for (let i = 0; i < tmp_tables2[0].length; i++) {                 
                series.push({name:yrs,data:[tmp_tables2[0][i]['RM30'],tmp_tables2[0][i]['RM50'],tmp_tables2[0][i]['RM100'],tmp_tables2[0][i]['RM150'],tmp_tables2[0][i]['RM200']]});
            }
        }
        yrs='2023'
        query ="SELECT LEFT(bizdate, 4)bizdateyear,SUM(RM30)RM30,SUM(RM50)RM50,SUM(RM100)RM100,SUM(RM150)RM150,SUM(RM200)RM200 FROM(";
        query +=" SELECT bizdate, location, MAX(RM30) RM30, MAX(RM50) RM50, MAX(RM100) RM100, MAX(RM150) RM150, MAX(RM200) RM200 FROM (";
        query +=" SELECT LEFT(bizdate, 7) bizdate, location,SUM(costmargin)costmargin,";
        query +=" IF(LEFT(description, 11) = 'HAMPER_RM30', SUM(qty), 0) RM30,";
        query +=" IF(LEFT(description, 11) = 'HAMPER_RM50', SUM(qty), 0) RM50,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM100', SUM(qty), 0) RM100,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM150', SUM(qty), 0) RM150,"
        query +=" IF(LEFT(description, 12) = 'HAMPER_RM200', SUM(qty), 0) RM200"
        query +=" FROM frontend.poschild"
        query +=" WHERE bizdate BETWEEN '"+yrs+"-01-01' AND '"+yrs+"-12-31'";
        query +=" AND Location IN ("+location+")";
        query +=" AND barcode IN ("+barcode+")";
        query +=" AND price IN(30,50,100,150,200)";
        query +=" GROUP BY price,barcode,location,LEFT(bizdate,7)) A";  
        query +=" GROUP BY LEFT(bizdate,7),location)a ORDER BY bizdate";
        tmp_tables2 = await connPanda.query(query); 
        if(await tmp_tables2[0].length>0){
            for (let i = 0; i < tmp_tables2[0].length; i++) {                 
                series.push({name:yrs,data:[tmp_tables2[0][i]['RM30'],tmp_tables2[0][i]['RM50'],tmp_tables2[0][i]['RM100'],tmp_tables2[0][i]['RM150'],tmp_tables2[0][i]['RM200']]});
            }
        }
        categories=['RM30','RM50','RM100','RM150','RM200'];
        await connWeb.query("REPLACE INTO tbl_charts(code,series,categories)VALUE('"+code+"','"+JSON.stringify(series)+"','"+JSON.stringify(categories)+"')");                 
        await connWeb.query("REPLACE INTO tbl_datatables(code,payload)VALUE('"+code+"','"+escapedJsonString+"')");            
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }    
    console.log('Populating '+location+' Done');     
}
async function process_sales(){
    const Location="'BDS','DGO','KHM','KNG','M88','MGT','MLN','PPR','PR2','PTS','SJT','SPG','TLP','TRN','SBBK','SRGD','STLB','SLMW','SPRS','STPP'";
    const location2="'GBVL','GDGO','GDMP','GHTP','GINN','GINT','GKBT','GKMS','GKPP','GLTJ','GPLC','GPPR','GPTG','GT2A','GTWR'";
    const barcode = "'1040044','868931','1038119','348649','912695','1009435','520108000501','911610','869442','520108000109','520108290012','1028646','915733','920087','920171','1007170','800296','982660','997920','761544','1030065','872599','872529','1036877','871640','1015001','520108000263','520108000285','921865','910966','982422','1015495','1037777','1029820','1029778','1030066','1020015','1020975','1040019','838551','1020074','1015341','1038341','1022323','1022522','1038587','1016031','520108000232','1038342','520108000151','520108000100','1041077','55503','911911','868903','981435','869267','1028647','964978','520108000305','919177','1016093','735833','520108000383','1039116','1041213','1037775','1036991','1019916','1039221','1038598','315161','965111','1020974','912688','1028763','1028556','1038014','834442','1015342','987014','520108000343','520108000358','1035978','1028728','1016032','964642','1037934','55475','520108000038','1020909','520108000310','909755','520108000360','1041541','1028648','912156','520108000384','970494','995834','1038891','1037336','1040594','1013404','1015343','1015161','1014128','1029811','1041365','520108000363','501111','1020915','913409','520108000107','1028649','920101','1020297','913283','1014482','1037774','1039195','1023349','234262','912996','973203','1037698','901705','1020273','790755','520008000030','981190','520108000036','868595','868924','520108290010','1028644','369397','912079','911659','520108000019','1028250','373849','1011580','909762','913276','964663','880565','1028572','995841','1028924','372526','910182','918225','1022436','870198','1040153','761313','1028251','1037773','520108000048','1038839','761299','919191','1016058','1028697','836696','837697','1007446','913115','1029816','356482','917966','838131','1013580','1039122','1039815','520108000388','367864','420203','967211','1016115','1027600','839370','1037697','520108000010','870114','910210','520108000013','1007476','909559','868959','965762','520108290011','1028645','754943','916755','912709','980378','962794','1020914','520108000399','1040593','1016114','871913','1038959','1014864','520108000043','1015561','815899','872592','1038105','355894','1037236','1035977','918645','837263','800121','869820','1015494','1037812','520108000193','1038569','902076','967218','520108000270','1037555','1013059','915698','870884','870037','1029615','392392','914970','1020146','1028639','911624','1038834','761684','914550','1009434','860195','914557','871619','520108000249','870044','1030064','1038599','912086','761215','1022362','1040654','1037811','1004080','1037554','1011485','910959','1037776','1015269','962395','838705','836388','761551'";
    await process_data('CKSR_HAMPER_2024',barcode,Location);
    await process_data('CKSM_HAMPER_2024',barcode,location2);
    process.exit();
}
process_sales();
