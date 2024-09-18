const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');

const port = process.env.port;
const host = process.env.host;
const dbname = process.env.dbname;
const username = process.env.u_name;
const password = process.env.password;
let isget_lastsync=1,ProcessDisplay={};
const { subMonths, getDaysInMonth, setDate, format } = require('date-fns');
const nodemailer = require('nodemailer');

const dbpool= mysql2.createPool({
	host: host,port: port,user: username,password: password,database:dbname,
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
const dbpool2= mysql2.createPool({
	host: 'cksgroup.my',port: '3306',user: 'cksroot',password: '4h]k53&[ugwN',database:'cksgroup_intra',
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
async function get_table_schema(table_name,database_name,conn){try{let field_name="",qry="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+database_name+"' AND TABLE_NAME='"+table_name+"'",tbldata=await conn.query(qry);for(let i=0;i<tbldata[0].length;i++)field_name+=tbldata[0][i]['COLUMN_NAME']+",";return field_name=field_name.slice(0,-1),field_name}catch(error){return console.log(error),!1}}
let transporter = nodemailer.createTransport({
    host: 'mail.cksgroup.my',
    port: 587,
    secure: false,     
    auth: {
      user: 'ivon.carter@cksgroup.my',
      pass: '@rcH_1301'
    },
    tls: {
        rejectUnauthorized: false 
    }    
});
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
function getCurrentWeek (date) {
    const year = date.getFullYear();
    const month = date.getMonth();
    const firstDayOfMonth = new Date(year, month, 1);    
    const firstSaturday = new Date(firstDayOfMonth);
    firstSaturday.setDate(7 - firstDayOfMonth.getDay());    
    let weekStart = new Date(firstDayOfMonth);
    let weekEnd = new Date(firstSaturday);    
    let week = 1;    
    while (date > weekEnd) {
        weekStart.setDate(weekEnd.getDate() + 1);
        weekEnd.setDate(weekEnd.getDate() + 7);        
        if (weekEnd.getMonth() !== month) {
            weekEnd = new Date(year, month + 1, 0);
        }        
        week++;
    }    
    return {
        weekNumber: week,
        weekStart: weekStart.toISOString().split('T')[0],
        weekEnd: weekEnd.toISOString().split('T')[0]
    };
};
async function process_data(salestype,Array_barcode){    
    let connPanda,connWeb,qry=''    
    const date = new Date();
    const today = new Date();
    const TomorowsDate = new Date();//TomorowsDate.setDate(today.getDate() + 1);
    const dayOfWeek = TomorowsDate.getDay();
    let _lastMonthDate = getLastMonthSameDate(TomorowsDate);      
    const LstMonth=_lastMonthDate.toISOString().split('T')[0].slice(0,7);  
    const dateA = getCurrentWeek(TomorowsDate);let AvgWeek="";
    switch (dateA.weekNumber) {
        case 1:        
            AvgWeek="'"+LstMonth+"-01' AND '"+LstMonth+"-07'";
        break;
        case 2:        
            AvgWeek="'"+LstMonth+"-08' AND '"+LstMonth+"-15'";
        break;        
        case 3:        
            AvgWeek="'"+LstMonth+"-16' AND '"+LstMonth+"-23'";
        break;
        case 4:        
            AvgWeek="'"+LstMonth+"-24' AND '"+LstMonth+"-30'";
        break;
    }
    // console.log(TomorowsDate.toISOString().split('T')[0]);
    // console.log(dayOfWeek);
    // process.exit();
    let Concept='FH';
    let propose_percentage = 1.2;
    let Arr_CKSR = ['BDS','DGO','KHM','KNG','M88','MGT','MLN','PPR','PR2','PTS','SJT','SPG','TLP','TRN'];
    let Arr_CKSM = ['GBVL','GDGO','GDMP','GHTP','GINN','GINT','GKBT','GKMS','GKPP','GLTJ','GPLC','GPPR','GPTG','GT2A','GTWR','STPP','STLB','SRGD','SPRS','SLMW','SBBK'];
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        let htmls_hdr="<tr style='border: 1px solid black; border-collapse: collapse;'><th style='border: 1px solid black; border-collapse: collapse;'>Location</th><th style='border: 1px solid black; border-collapse: collapse;'>Barcode</th><th style='border: 1px solid black; border-collapse: collapse;'>Descrption</th><th style='border: 1px solid black; border-collapse: collapse;'>QTY</th></tr>";
        let htmls="";let email_htmls="";
        qry="SELECT `code` FROM location WHERE LEFT(`code`,1)<>'f' and LEFT(`code`,2)<>'DC' and `code`not in ('GDCF','GDC','HQ') limit 5";
        const tbl_location = await connWeb.query(qry); 
        if(await tbl_location[0].length>0){
            for (let j = 0; j < tbl_location[0].length; j++) {  
                const _location= tbl_location[0][j]['code'];

                for (let i = 0; i < Array_barcode.length; i++) {   
                    const _barcode=Array_barcode[i];let query="";
                    console.log('Barcode : '+_barcode+' Populating : '+_location);
                    let __description = await connPanda.query("SELECT b.itemcode,a.description FROM itemmaster a INNER JOIN itembarcode b ON a.itemcode=b.itemcode WHERE barcode='"+_barcode+"'");            
                    let _description='';
                    if(__description[0][0]['description']!=undefined){
                        _description=__description[0][0]['description'].replace(/'/g, "\\'");
                    }
                    const _itemcode= __description[0][0]['itemcode'];
                    qry="SELECT ROUND(SUM(SoldWeight)/7)*1.1 AS MinQty,ROUND(SUM(SoldWeight)/7)SoldWeight FROM(";
                    qry+=" SELECT IF(SoldbyWeight=1,TRUNCATE(weightvalue,2),TRUNCATE(qty,2)) SoldWeight";
                    qry+=" FROM frontend.poschild a";
                    qry+=" INNER JOIN backend.itembarcode b ON b.itemcode=a.itemcode";
                    qry+=" WHERE b.itemcode='"+_itemcode+"'";
                    qry+=" AND location ='"+_location+"'";                    
                    qry+=" AND bizdate BETWEEN "+AvgWeek;
                    qry+=" GROUP BY bizdate,refno,line)A";                    
                    let MinQty = await connPanda.query(qry);                    
                    let SoldWeight=0;
                    if(await MinQty[0].length==0){MinQty=0;}else{SoldWeight=MinQty[0][0]['SoldWeight'];MinQty=MinQty[0][0]['MinQty'];}
                    if(MinQty==null){MinQty=0;}if(SoldWeight==null){SoldWeight=0;}
                    if(Arr_CKSR.includes(_location)) {
                        if(MinQty<3){MinQty=3;}Concept='CKSR';
                    }
                    if(Arr_CKSM.includes(_location)) {
                        if(MinQty<3){MinQty=1;}Concept='CKSM';                        
                    }                    

                    qry="SELECT round(qoh)qoh FROM itemmaster_branch_stock WHERE itemcode='"+_itemcode+"' AND branch='"+_location+"'";                    
                    let CurrentQOH = await connPanda.query(qry);                    
                    if(await CurrentQOH[0].length==0){CurrentQOH=0;}else{CurrentQOH=CurrentQOH[0][0]['qoh'];}
                    if(CurrentQOH<=0){CurrentQOH=0;}
                    let proposeqty=MinQty-CurrentQOH;
                    if(proposeqty<0){proposeqty=0;}
                    if(await _itemcode=='2050323' && proposeqty<MinQty){MinQty=6;}
                    proposeqty=Math.ceil(proposeqty);
                    let querry=" update custom_sales set avgweek="+MinQty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);                    
                    switch (dayOfWeek) {
                        case 2:
                            querry=" update custom_sales set tue_soldweight=0,tue_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set tue_soldweight="+CurrentQOH+",tue_proposeqty='"+proposeqty+"' where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;
                        case 0:
                            querry=" update custom_sales set sun_soldweight=0,sun_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set sun_soldweight="+CurrentQOH+",sun_proposeqty='"+proposeqty+"' where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;                               
                        case 1:
                            querry=" update custom_sales set mon_soldweight=0,mon_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set mon_soldweight="+CurrentQOH+",mon_proposeqty="+proposeqty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;
                        case 3:
                            querry=" update custom_sales set wed_soldweight=0,wed_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set wed_soldweight="+CurrentQOH+",wed_proposeqty="+proposeqty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;    
                        case 4:
                            querry=" update custom_sales set thu_soldweight=0,thu_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set thu_soldweight="+CurrentQOH+",thu_proposeqty="+proposeqty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;
                        
                        case 5:
                            querry=" update custom_sales set fri_soldweight=0,fri_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set fri_soldweight="+CurrentQOH+",fri_proposeqty="+proposeqty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;
                        case 6:
                            querry=" update custom_sales set sat_soldweight=0,sat_proposeqty=0 where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                            querry=" update custom_sales set sat_soldweight="+CurrentQOH+",sat_proposeqty="+proposeqty+" where location='"+_location+"' and barcode='"+_barcode+"'";await connWeb.query(querry);
                        break;                        
                    }
                    htmls+="<tr style='border: 1px solid black; border-collapse: collapse;'><td style='border: 1px solid black; border-collapse: collapse;padding:10px;'>"+_location+"</td><td style='border: 1px solid black; border-collapse: collapse;padding:10px;'>"+_barcode+"</td><td style='border: 1px solid black; border-collapse: collapse;padding:10px;'>"+_description+"</td><td style='border: 1px solid black; border-collapse: collapse;text-align:center;'>"+proposeqty+"</td></tr>";            
                }
                email_htmls +="<table style='border: 1px solid black; border-collapse: collapse;width:100%;'>"+htmls_hdr+htmls+"</table><br><br>";htmls="";
            }
        }       
        await connWeb.commit();
        await SendEmail("<h4>Order for : "+today.toISOString().split('T')[0]+"</h4>"+email_htmls,salestype);
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
        process.exit();
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }        
}
console.clear();
async function process_sales(){
    const tmpstamp = new Date();console.log('Populating process_data : '+tmpstamp);    
    // const Array_Vege=['2051920','2051917','2051626','2051627','2051921','2051912','2051918','2051105','2050214','2050220','2050323','2050213','2050212','2050211','2051216','2051219','2050210','2051913','2051927'];
    const Array_Vege=['2050212','2051917','2051626',];        
    console.clear();console.log('Processing Vege');
    await process_data('Vege',Array_Vege);  
    console.log('Processing END');

    // const Array_Fruits=['410822000020','410811000012','410812000010','2087221','2050229','2050222','2051601','2050706','2051215','2051620','2052205','2052206','2052217','2052221','2050801','2051617','2052507','2051348','2051218','2051217','2051901','2074971','2050402','2050117','2051341','2050410','2051224','2051226','2050374','2095532','410816000010','2051902','410703000020','2051106','2051631','2000613','2000614','2051004','2051019','2051303','2051948','2051312','2050227','2050217','2000118','2051656'];
    // console.log('FRUITS - Processing');  
    // await process_data('FRUITS',Array_Fruits);  
    // console.log('FRUITS - Done');

    // const Array_VegeKundasan=['2051905','2051621','2051625','2091978','2051937','2051936','2051603','2051103','2051915','2032304','2052001','2051919','2051934','2050344','2051206','2051903','2050325','2050329','2051504','2051502','2051201','2050301','2051814','2051924','2050341','2076797','2001039','2001122','2052004','2052005','2052015','2050326','2052601','2051381','2001041','2081126','2051210','2050204','2050209','2051930','2030601','2030602','2051300','2051346','2050370','2022940','2001038','2051403','2050116','2051602','1039564'];
    // console.log('VegeKundasan - Processing');  
    // await process_data('VegeKundasan',Array_VegeKundasan);  
    // console.log('VegeKundasan - Done');

    // const Array_seafood=['2001189','2001207','2001209','2001256','2001257','2001258','2001265','2001395','2001432','2001534','2081315','2081326'];
    // console.log('SeaFood - Processing');  
    // await process_data('SeaFood',Array_seafood);  
    // console.log('SeaFood - Done');
    
    // const Array_ImportFruits=['6001651061994','6001651155716','2000330','2000315','6974955821130','2001526','411281108800','411280405000','411280405500','411280406500','411280412500','411210407200','410030316500','410030318000','410083316500','410083319800','410083321600','410085710000','410057811000','410057809000','410057812000','6225000290143','6225000373112','411332304800'];
    // console.log('ImportedVege - Processing');  
    // await process_data('ImportedVege',Array_ImportFruits);  
    // console.log('ImportedVege - Done');       

    process.exit();
}
process_sales();

async function SendEmail(html, subject) {
    let mailOptions = {
      from: 'ivon.carter@cksgroup.my',     
      to: 'ivonworkmail@gmail.com',        
      subject: subject,                    
      html: html                          
    };  
    console.log('Email sending:');  
    try {      
      let info = await transporter.sendMail(mailOptions);
      console.log('Email sent: ' + info.response);
      return true; // Return true on success
    } catch (error) {
      console.error('Error sending email:', error);
      return false; // Return false on error
    }
  }
