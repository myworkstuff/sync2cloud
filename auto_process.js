const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const { format, startOfYear, startOfMonth, endOfMonth, addMonths } = require('date-fns');

const CronJob = require('cron').CronJob;
const { exec } = require('child_process');
console.clear();
console.log('Auto Run started');
const jobs = new CronJob("0 8 * * *", async function() {
    exec('node rpt_hamper.js', (error, stdout, stderr) => {
        console.log('Running rpt_hamper.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_hamper Output: ${stdout}`);
    });
    exec('node rpt_BE2501.js', (error, stdout, stderr) => {
        console.log('Running rpt_BE2501.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_BE2501 Output: ${stdout}`);
    });
    exec('node rpt_promotion_insentive.js', (error, stdout, stderr) => {
        console.log('Running rpt_promotion_insentive.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_promotion_insentive Output: ${stdout}`);
    });     
});
jobs.start();