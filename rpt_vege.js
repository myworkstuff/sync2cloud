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
            qry="SELECT `code` FROM location WHERE LEFT(`code`,1)='f' and LEFT(`code`,2)<>'DC' and `code`not in ('GDCF','GDC','HQ')";
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
                    if(MinQty<3){MinQty=1;}                     
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
                        }
                        if(proposeqty<MinQty){
                            proposeqty=MinQty;                                                      
                        }                        
                        // proposeqty = await (customRound(proposeqty));
                        proposeqty=Math.ceil(proposeqty)
                        if(SoldWeight==null || SoldWeight==''|| SoldWeight=='null'){SoldWeight=0;}                        
                        // console.log(dayOfWeek2);
                        switch (dayOfWeek2) {
                            case 2:
                                querry=" insert into custom_sales(sales_period,salestype,location,barcode,description,tue_soldweight,tue_proposeqty)"
                                querry+="value"                    
                                querry+=" ('"+lastMonthDate+"','"+salestype+"','"+_location+"','"+_barcode+"','"+_description+"',"+SoldWeight+","+proposeqty+")"; 
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
                            case 0:
                                querry=" update custom_sales set sun_soldweight="+SoldWeight+",sun_proposeqty='"+proposeqty+"'"
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'"                                                    
                                await connWeb.query(querry);
                            break;                               
                            case 1:
                                querry=" update custom_sales set mon_soldweight="+SoldWeight+",mon_proposeqty="+proposeqty
                                querry+=" where location='"+_location+"' and barcode='"+_barcode+"'";                                
                                await connWeb.query(querry);
                            break;                                                       
                        }
                    }//process.exit();
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

const Array_Fruits=['411114780010','411114780011','411713011130','411713011134','2000035','2000494','411713010880','6957937482222','6774432369069','2000074','2000108','2000109','1010998','411713011000','6958851400088','411713011004','41133200002','2000112','1011611','2000118','6001651398755','6936489100082','1013597','1013598','1013599','1013602','411332100002','6936489102031','1015924','5289000101923','1015975','7804674660002','6225000377714','6225000377721','6971893150247','1018108','2000355','2000366','2000447','2000385','6970717980053','6970717980008','9555604800507','9555604803898','2000400','1022794','2000409','2000411','2000346','2000368','2000413','2000410','9555104600386','1022802','1022808','9555804501419','812049006109','1022812','1022814','1022815','1022816','9555604800521','9555604802037','9555604800699','9555604800545','9555590406141','9555104600393','9555104600362','9555604800019','1022831','9555604801085','9555604802754','1022834','1022835','9555604800682','9554100395609','9555604802464','9555604800071','9555604802112','1022841','9555604800026','9555604803164','9555604803188','9555604801030','9555604800002','1022847','9555604802693','1022849','9555604802143','9555604801047','9555604800613','9555604801412','1022856','1022857','1022859','6970601451294','9555604802440','9555604800538','9555604800569','9554100063980','9555604803102','1022876','1022878','9555604802839','1022880','9554100064208','9555604801504','6936489100099','9555604800101','1022888','1022889','9555604801191','9555604805328','9555604802358','9555349101518','9555604804420','9555604800514','9555604801818','1022901','9555604800736','9555604801016','9555604800842','9555604800637','1022907','9555604802907','9555604800941','1022911','6936489102093','9555604800194','9555604801115','6953150183571','9555941400019','9555604802785','9555604802952','9555604804666','9555604801160','9555604801092','6936489101966','9555604802570','9555604801108','9555604802792','9555604800743','1022934','9555604800651','9555604801849','9555604800712','9555604804383','9555604802471','9555604803904','1022946','9555604801177','9555604800095','1022952','9555604802273','1022955','6956599600036','9555604800835','1022959','9555604802808','9555604801931','9555604800996','9555604800149','9555604801580','9555604802136','1022972','9555604802068','9555604802945','1022975','9555604802624','6009678410023','9555604802365','9555604803874','6975802080168','1022989','1022990','9555604800668','9555604801856','9555604806585','9555604802938','9555604803126','9555604801153','9555604802013','1023000','300005','1023001','1023002','1023003','1023004','9555604806561','6224009024117','9555604803195','9555604800088','1023010','6944546169361','9555604802082','6936489102048','9555604800033','9555941400026','9555604801764','9555604802150','6936489103281','9555604803218','9555604802402','9555604806226','1023027','9555604800675','1023030','9555604803423','1023038','9555604801597','1023040','1023041','1023042','9555604800927','9555604803676','9555604803966','9555959900129','6972950860017','9555604806295','1023056','8421747006073','6009900306797','8908018974011','9555604804659','9555604800903','9555604804413','9555604803416','9555604801276','9555604804635','1023070','1023071','9555604802372','9555604800897','1023074','9555604800576','9555604805311','9555604800804','9555604800958','9555604800750','9555604801207','9555604804390','9555604802556','6956599600043','6009900099644','9555604802983','8437014044232','9555604800910','6225000290143','9555604802723','1023094','9555604802129','9555604803027','9555604804642','9555604803355','9555604801467','9555604800729','9555604801733','9555604801078','8858836255109','9555604803256','9555604806554','9555104600423','9555604800811','9555604803478','9555604801146','8855718000017','9555604802716','1023122','9555604803041','1023125','1023128','9555604801795','8858836201106','1023131','9555604800583','9555604800590','9555604801009','6955938100084','9555604802020','1023141','9555604802921','9555604801139','1023144','9555604802075','9555604803980','1023150','9555604804901','2000437','1023172','2000422','2000421','2000467','2000486','2000429','2000548','2000470','1023181','1023182','1023183','1023185','2000483','1023189','2000436','2000480','2000462','1023193','2000484','2000490','2000415','2000451','2000417','1023201','2000445','2000463','2000454','1023206','2000443','1023209','2000546','2000448','2000547','1023214','1023217','1023219','2000473','2000434','1023222','1023223','2000466','2000438','2000474','2000449','1023231','1023234','2000501','1023236','2000506','2000543','1023242','1023243','2000458','2000414','2000537','2000459','2000435','2000461','2000549','2000431','2000504','2000489','2000485','2000465','2000478','2000432','2000418','2000419','2000550','2000544','1023268','2000509','1023272','2000457','2000469','2000487','2000440','2000475','2000479','2000442','2000540','1023283','2000507','2000545','1023286','1023287','2000439','1023289','2000464','2000452','2000430','2000426','2000427','2000456','1023297','2000444','2000425','2000420','1023301','2000488','2000542','2000471','2000472','2000441','2000539','2000538','2000482','2000460','2000477','2000455','2000476','2000450','2000423','1023319','2000536','2000541','1023322','2000446','1023326','2000481','1023329','1023330','2000433','1023332','2000428','2000468','2000453','1023337','2000424','9555604806363','9555604800491','9555604805465','9555604800378','1024637','300074','1024640','300037','9555604800606','008290','1024665','1024699','300026','1024801','300014','9554100395616','9555604802389','1024844','300036','9555174701884','1026948','9551011840112','1027002','1027029','1027032','1027040','2000574','2000731','2000592','2000565','2000741','2000588','2000569','2000571','2000587','2000591','2000742','2000590','2000576','2000594','2000581','2000583','2000575','2000572','2000586','2000582','2000734','2000584','2000593','2000744','2000577','2000589','2000573','2000580','2000579','1027242','1027243','1027244','300032','2000611','6944639060032','1027284','1027285','1027286','1027287','1027288','9555804500580','1027290','6974890300028','2000777','2000756','2000779','2000780','2000778','2000781','2000782','2000783','2000784','2000785','2000786','2000787','1027326','2000788','2000789','2000218','2000372','2000791','2000790','2000793','2000795','2000796','2000797','1027357','9555604806564','1027359','2000798','2000612','6953150183052','8859544300006','6953150183045','9555604803560','1028962','6955778050051','695778050051','1029826','1029903','1029966','1030176','2000625','1036020','1036071','1036072','6970523679653','1036581','6957450100016','6973637650075','1037471','6957937482246','1037560','1037790','9554100344409','1038310','1038311','6949264100290','6953150183656','1038637','1038645','6976532310044','1038933','1038934','8855718001007','1039121','6970777740109','6970777740093','6958191500264','6973637650266','9555604803171','6970791812257','9555604806592','9555604806622','1041706','6958191510010','1042244','9555604801269','6931787400162','9320804000398','2030304','2030314','2030319','2050117','2050206','2050211','2050212','2050213','2050214','2050222','2050230','2050233','2050236','2050303','2050305','2050307','2050308','2050310','2050322','2050323','2050325','2050329','2050332','2050334','2050335','2050336','2050379','2050385','2050390','2050402','2050405','2050701','2050706','2051004','2051007','2051106','2051113','2051205','2051216','2051217','2051218','2051219','2051220','2051224','2051226','2051228','2051235','2051312','2051349','2051381','2051504','2051553','2051601','2051615','2051617','2051637','2051656','2051711','2051806','2051901','2051902','2051907','2051912','2051918','2051920','2051921','2052015','2052205','2052208','2052217','17283','2065053','411332100008','411713010884','411690000020','2022940','411230410010','6953150100677','470807000014','410703000021','334593','410703000010','410703000012','410703000020','410816000010','9555604802051','410822000020','410916000010','411230408800','411230408806','411230410006','8421747004151','411600100012','2076797','6936489101218','470807000010','400379','2083455','6009529399972','420196','6957937480051','6957937480686','9705859801358','500949','501003','2051359','0061379','6930299220084','71631','6930299220107','2051215','2035896','6936489100006','6936489101010','6009900099620','6936489101089','410714000011','411230410000','74298','6936489101348','761992','411230408804','2099344','795032','2052507','800107','2051234','2051257','811146','411230411300','411332100000','2052509','6955938100077','41133200000','2050840','869659','869876','411780110000','902503','411730111300','411730111304','411730111306','411230410004','2095455','411730108800','411730108804','411730108806','411230411306','6971893150018','410918000011','933695','411780110004','6936489102000','6774432369076'];
    // console.log('CSC VEGE - Processing');  
    // await process_data('CSC VEGE',Array_Fruits);  
    // console.log('CSC VEGE - Done');
    
    const Array_VegeKundasan=['2000035','2000494','6958851400071','1013602','6971286740239','6936489102031','6971893150247','6974434700086','2000366','6971533452014','6973637650068','6970717980053','6970717980008','2000400','2000411','9555104600386','9555104600393','1022835','9554100395609','1022849','1022888','1022889','1022907','6936489102093','1022934','1022952','1022955','6956599600036','1022990','1023000','300005','6936489102048','9555941400026','6936489103281','9555604806226','9555604800576','9555604804390','8858836255109','8855718000017','8858836201106','6955938100084','2000422','2000467','2000486','2000470','2000483','2000451','2000445','2000454','2000473','2000434','2000438','2000474','2000449','2000414','2000435','2000489','2000469','2000440','2000479','2000442','2000507','2000439','2000444','2000471','2000472','2000482','2000455','2000450','2000433','2000468','2000453','1024384','300025','1024637','300074','1024640','300037','1024699','300026','9554100395616','1024844','300036','9555174701884','1027002','1027040','2000574','2000731','2000592','2000565','2000588','2000569','2000571','2000587','2000590','2000594','2000581','2000583','2000586','2000734','2000584','2000593','2000577','2000589','2000573','2000580','2000579','1027242','1027244','300032','2000611','6944639060032','2000779','2000780','2000778','2000782','1027326','2000791','2000793','2000795','2000797','1027359','6953150183052','8859544300006','6953150183045','1030176','1030177','1036020','6957450100016','6973637650075','9551020400321','9551020400338','9551020400611','9551020400345','9551020401793','9551020400499','9551020400512','9551020400505','9551020400260','9551020400277','9551020400840','9551020401021','9551020401014','9551020400987','9551020400758','9551020400796','9551020400741','9551020400291','9551020400284','9551020400543','9551020400864','9551020400802','9551020400857','9551020400475','9551020400420','9551020400963','9551020400444','9551020401809','9555615201881','1037446','9551020401922','9551020401939','9555615202888','4897013640165','1037461','9557825735026','1037490','9551020401540','1037491','9551020401892','9551020401908','9551020400390','9551020400598','1037513','9551020401595','1037526','1037529','9551020401946','1037560','1037561','1037562','1037563','1037567','9551020401960','9551020401038','1037588','9551020401618','9551020401427','9551020400239','9551020400222','9551020401434','6973637650242','9551020400215','9551020400581','6925307588867','6974434700079','9551020400208','1037689','9551008410021','9555452101603','1037790','1037987','1037988','1038018','1038053','6973637650051','1038118','2000675','9551020402110','6958191500059','1038260','2001419','2001434','1038350','1038504','1038584','1038622','6953150183656','6953150183359','1038637','6953150183663','6936489100853','6976532310044','1038811','1038825','1038930','1038931','1039002','1039103','1039104','9705859800160','1039105','1039121','6970777740109','6970777740093','2001469','6958191500264','6958191500042','6973637650266','1039276','9551020400482','1040107','2001532','1040207','1040208','6970791812257','6974581960074','6936489132915','1041822','1042018','6958191510010','6931787400162','1042722','2030319','2050211','2050212','2050213','2050230','2050233','2050323','2050325','2050329','2050332','2050334','2050701','2050706','2051106','2051113','2051202','2051217','2051218','2051219','2051504','2051615','2051901','2051902','2051907','2051912','2051921','2052015','2052201','17283','9555046900162','6953150100677','470807000014','410703000021','410703000010','410703000012','410916000010','350861','470807000010','6930274615058','2083455','420196','6957321500099','0061379','6930299220084','71631','6930299220107','2051215','2035896','6936489101010','6936489101027','6936489101089','80192','6955938100077','869876','6958851401016','6971893150018','6953150182703','6936489102000','6972913850024'];
    console.log('LH VEGE - Processing');  
    await process_data('LH VEGE',Array_VegeKundasan);  
    console.log('LH VEGE - Done');     

    process.exit();
}
process_sales();