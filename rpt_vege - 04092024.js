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
const { subMonths, getDaysInMonth, setDate, format } = require('date-fns');


const dbpool= mysql2.createPool({
	host: host,port: port,user: username,password: password,database:dbname,
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
const dbpool2= mysql2.createPool({
	host: 'cksgroup.my',port: '3306',user: 'cksroot',password: '4h]k53&[ugwN',database:'cksgroup_intra',
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
async function get_table_schema(table_name,database_name,conn){try{let field_name="",qry="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+database_name+"' AND TABLE_NAME='"+table_name+"'",tbldata=await conn.query(qry);for(let i=0;i<tbldata[0].length;i++)field_name+=tbldata[0][i]['COLUMN_NAME']+",";return field_name=field_name.slice(0,-1),field_name}catch(error){return console.log(error),!1}}
function getFirstDecimalPointValue(num) {
    const numStr = num.toString();
    const [integerPart, decimalPart] = numStr.split('.');
    if (decimalPart && decimalPart.length > 0) {
        return parseInt(decimalPart[0], 10);
    }
    return 0;
}
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}

function getLastMonthSameDate(date) {
    const previousMonthDate = subMonths(date, 1);
    const daysInPreviousMonth = getDaysInMonth(previousMonthDate);    
    const adjustedDate = setDate(previousMonthDate, Math.min(date.getDate(), daysInPreviousMonth));
    return adjustedDate;
}
const getDateOfWeekday = (dateString, dayOfWeek) => {
    const date = new Date(dateString);
    const currentDay = date.getDay();
    const difference = (dayOfWeek - currentDay + 7) % 7;
    date.setDate(date.getDate() + difference);
    return date;
};
// const getNextWeekDay = (dateString,dayOfWeek) => {
//     const today = new Date(dateString);
//     const currentDay = today.getDay();
//     const daysUntilNextWeek = (7 - currentDay + dayOfWeek) % 7 + 7;
//     today.setDate(today.getDate() + daysUntilNextWeek);
//     return today;
// };
const getNextWeekDay = (dateString, dayOfWeek) => {
    const today = new Date(dateString);
    const currentDay = today.getDay();        
    let daysUntilNextWeek = (dayOfWeek - currentDay + 7) % 7;        
    if (daysUntilNextWeek === 0) {
        daysUntilNextWeek += 7;
    }    
    today.setDate(today.getDate() + daysUntilNextWeek);
    return today.toISOString().split('T')[0];
};
async function customRound(number) {
    const integerPart = Math.floor(number);
    const decimalPart = number - integerPart;

    if (decimalPart >= 0.35 && decimalPart < 0.5) {
        return integerPart + 0.5;
    } else if (decimalPart >= 0.6 && decimalPart <= 0.9) {
        return Math.ceil(number);
    } else {
        return Math.floor(number);
    }
}
async function process_data(salestype,Array_barcode){    
    let connPanda,connWeb,qry=''    
    const date = new Date();
    const today = new Date();
    const dayOfWeek = today.getDay();
    const diffToMonday = (dayOfWeek === 0 ? -6 : 2) - dayOfWeek;
    let currentTuesday = new Date(today);currentTuesday.setDate(today.getDate() + diffToMonday);currentTuesday=currentTuesday.toISOString().split('T')[0];        
    const daysToAdd = 7;
    let _currentDate = new Date(currentTuesday);
    let _lastMonthDate = getLastMonthSameDate(_currentDate);//console.log(_lastMonthDate);process.exit();  
    let _NextWeekTue = getNextWeekDay(_lastMonthDate,2);//console.log(_NextWeekTue);process.exit();  
    let _NextWeekMonday= new Date(_NextWeekTue);
    _NextWeekMonday.setDate(_NextWeekMonday.getDate() + 6);
    _NextWeekMonday=_NextWeekMonday.toISOString().split('T')[0];
    // console.log(_NextWeekTue);console.log(_NextWeekMonday);process.exit();  
    const startDate = new Date(currentTuesday);
    let Concept='FH';
    let propose_percentage = 1.2;
    let Arr_CKSR = ['BDS','DGO','KHM','KNG','M88','MGT','MLN','PPR','PR2','PTS','SJT','SPG','TLP','TRN'];
    let Arr_CKSM = ['GBVL','GDGO','GDMP','GHTP','GINN','GINT','GKBT','GKMS','GKPP','GLTJ','GPLC','GPPR','GPTG','GT2A','GTWR','STPP','STLB','SRGD','SPRS','SLMW','SBBK'];
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        await connWeb.query("DELETE FROM custom_sales where salestype='"+salestype+"'"); 
        for (let i = 0; i < Array_barcode.length; i++) {      
            const _barcode=Array_barcode[i];
            // console.log('Barcode : '+_barcode)
            let query="";
            let __description = await connPanda.query("SELECT b.itemcode,a.description FROM itemmaster a INNER JOIN itembarcode b ON a.itemcode=b.itemcode WHERE barcode='"+_barcode+"'");
            let _description='';
            if(__description[0][0]['description']!=undefined){
                _description=__description[0][0]['description'].replace(/'/g, "\\'");
            }            
            const _itemcode= __description[0][0]['itemcode'];
            qry="SELECT `code` FROM location WHERE LEFT(`code`,1)<>'f' and LEFT(`code`,2)<>'DC' and `code`not in ('GDCF','GDC','HQ')";
            const tbl_location = await connWeb.query(qry); 
            if(await tbl_location[0].length>0){
                for (let j = 0; j < tbl_location[0].length; j++) {      
                    const _location= tbl_location[0][j]['code'];                    
                    // console.log('Barcode : '+_barcode+' Populating : '+_location);
                    qry="SELECT ROUND(SUM(SoldWeight)/7) AS MinQty FROM(";
                    qry+=" SELECT IF(SoldbyWeight=1,TRUNCATE(weightvalue,2),TRUNCATE(qty,2)) SoldWeight";
                    qry+=" FROM frontend.poschild a";
                    qry+=" INNER JOIN backend.itembarcode b ON b.itemcode=a.itemcode";
                    qry+=" WHERE b.itemcode='"+_itemcode+"'";
                    qry+=" AND location ='"+_location+"'";
                    qry+=" AND bizdate  BETWEEN '"+_NextWeekTue+"' AND '"+_NextWeekMonday+"'";
                    qry+=" GROUP BY bizdate,refno,line)A";
                    let MinQty = await connPanda.query(qry);
                    if(await MinQty[0].length==0){MinQty=0;}else{MinQty=MinQty[0][0]['MinQty'];}
                    if(MinQty==null){MinQty=0;}
                    if (Arr_CKSR.includes(_location)) {
                        if(MinQty<3){MinQty=3;}Concept='CKSR';
                    }
                    if (Arr_CKSM.includes(_location)) {
                        if(MinQty<3){MinQty=1;}Concept='CKSM';
                        // propose_percentage=1.15;
                    }                       
                    for (let i = 0; i < daysToAdd; i++) {     
                        let currentDate = new Date(startDate);       
                        currentDate.setDate(currentDate.getDate() + i);
                        let dayOfWeek2 = currentDate.getDay();
                        let tmpDate= new Date(currentDate.toISOString().split('T')[0]);
                        let lastMonthDate = format(getLastMonthSameDate(tmpDate), 'yyyy-MM-dd');
                        let NextWeekDay = getNextWeekDay(lastMonthDate,dayOfWeek2);
                        // console.log('Barcode : '+_barcode+' Populating : '+_location+' '+NextWeekDay);                        
                        // process.exit();
                        let querry=" SELECT ROUND(SUM(SoldWeight) * 1.1, 2) AS proposeqty,ROUND(SUM(SoldWeight), 2) AS SoldWeight FROM(";
                        querry+=" SELECT IF(SoldbyWeight=1,TRUNCATE(weightvalue,2),TRUNCATE(qty,2)) SoldWeight";
                        querry+=" FROM frontend.poschild  a";
                        querry+=" INNER JOIN backend.itembarcode b ON b.itemcode=a.itemcode";
                        querry+=" WHERE b.itemcode='"+_itemcode+"'";
                        querry+=" AND location ='"+_location+"' AND void=0";
                        querry+=" AND bizdate ='"+NextWeekDay+"'";
                        querry+=" GROUP BY bizdate,refno,line)A";                                         
                        let proposeqty = await connPanda.query(querry);
                        let SoldWeight=0;
                        if(await proposeqty[0].length>0){
                            if(await proposeqty[0].length==0){proposeqty=0;}else{SoldWeight=proposeqty[0][0]['SoldWeight'];proposeqty=proposeqty[0][0]['proposeqty'];}   
                        }else{
                            proposeqty=0;SoldWeight=0;                            
                        }
                        if(proposeqty==null || proposeqty==''|| proposeqty=='null'){proposeqty=0;}                        
                        if(proposeqty==0){
                            proposeqty=MinQty;
                            if(await _itemcode=='2050323'){proposeqty=6;}                            
                        }
                        if(proposeqty<MinQty){
                            proposeqty=MinQty;
                            if(await _itemcode=='2050323'){proposeqty=6;}                            
                        }                        
                        // proposeqty = await (customRound(proposeqty));
                        proposeqty=Math.ceil(proposeqty)
                        if(SoldWeight==null || SoldWeight==''|| SoldWeight=='null'){SoldWeight=0;}                        

                        switch (dayOfWeek2) {
                            case 2:
                                querry=" insert into custom_sales(sales_period,salestype,location,barcode,description,tue_soldweight,tue_proposeqty)"
                                querry+="value"                    
                                querry+=" ('"+lastMonthDate+"','"+salestype+"','"+_location+"','"+_barcode+"','"+_description+"',"+SoldWeight+","+proposeqty+")"; 
                                await connWeb.query(querry);
                            break;
                            case 0:
                                querry=" update custom_sales set sun_soldweight="+SoldWeight+",sun_proposeqty='"+proposeqty+"'"
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;                               
                            case 1:
                                querry=" update custom_sales set mon_soldweight="+SoldWeight+",mon_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;
                            case 3:
                                querry=" update custom_sales set wed_soldweight="+SoldWeight+",wed_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;    
                            case 4:
                                querry=" update custom_sales set thu_soldweight="+SoldWeight+",thu_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;
                            case 5:
                                querry=" update custom_sales set fri_soldweight="+SoldWeight+",fri_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;
                            case 6:
                                querry=" update custom_sales set sat_soldweight="+SoldWeight+",sat_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;                               
                        }              
                    }
                }
            }else{
                console.log('Nothing to process')
            }    
        } 
        await connWeb.commit();         
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
        process.exit();
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }    
    // console.log('Populating '+salestype+' Done');     
}
console.clear();
async function process_sales(){
    const tmpstamp = new Date();console.log('Populating process_data : '+tmpstamp);    
    // const Array_Vege=['2051920','2051917','2051626','2051627','2051921','2051912','2051918','2051105','2050214','2050220','2050323','2050213','2050212','2050211','2051216','2051219','2050210','2051913','2051927'];
    // console.log('[Anis] Vege - Processing');  
    // await process_data('Vege',Array_Vege);  
    // console.log('[Anis] Vege - Done');        

    // const Array_seafood=['2001189','2001207','2001209','2001256','2001257','2001258','2001265','2001395','2001432','2001534','2081315','2081326'];
    // console.log('SeaFood - Processing');  
    // await process_data('SeaFood',Array_seafood);  
    // console.log('SeaFood - Done');

    // const Array_Fruits=['410822000020','410811000012','410812000010','2087221','2050229','2050222','2051601','2050706','2051215','2051620','2052205','2052206','2052217','2052221','2050801','2051617','2052507','2051348','2051218','2051217','2051901','2074971','2050402','2050117','2051341','2050410','2051224','2051226','2050374','2095532','410816000010','2051902','410703000020','2051106','2051631','2000613','2000614','2051004','2051019','2051303','2051948','2051312','2050227','2050217','2000118','2051656'];
    // console.log('FRUITS - Processing');  
    // await process_data('FRUITS',Array_Fruits);  
    // console.log('FRUITS - Done');
    
    // const Array_VegeKundasan=['2051905','2051621','2051625','2091978','2051937','2051936','2051603','2051103','2051915','2032304','2052001','2051919','2051934','2050344','2051206','2051903','2050325','2050329','2051504','2051502','2051201','2050301','2051814','2051924','2050341','2076797','2001039','2001122','2052004','2052005','2052015','2050326','2052601','2051381','2001041','2081126','2051210','2050204','2050209','2051930','2030601','2030602','2051300','2051346','2050370','2022940','2001038','2051403','2050116','2051602','1039564'];
    // console.log('VegeKundasan - Processing');  
    // await process_data('VegeKundasan',Array_VegeKundasan);  
    // console.log('VegeKundasan - Done');

    // const Array_ImportFruits=['6001651061994','6001651155716','2000330','2000315','6974955821130','2001526','411281108800','411280405000','411280405500','411280406500','411280412500','411210407200','410030316500','410030318000','410083316500','410083319800','410083321600','410085710000','410057811000','410057809000','410057812000','6225000290143','6225000373112','411332304800'];
    // console.log('ImportedVege - Processing');  
    // await process_data('ImportedVege',Array_ImportFruits);  
    // console.log('ImportedVege - Done');       

    process.exit();
}
process_sales();