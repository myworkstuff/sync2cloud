const axios = require('axios');
const mysql = require('mysql2');
const fs = require('fs');
const path = require('path');

const zerotierToken = 'DyCLsoDJNYo7qZFLTuFW6B7iP7b9HDUZ';

// Set your ZeroTier network ID
const networkId = '1c33c1ced0ebf251'; 

// ZeroTier API base URL
const zerotierBaseURL = 'http://localhost:9993';

// Function to join ZeroTier network using the API
async function joinNetwork() {
  try {
    const response = await axios.post(
      `${zerotierBaseURL}/network/${networkId}`, 
      {},
      {
        headers: {
          Authorization: `Bearer ${zerotierToken}`,
        },
      }
    );
    console.log('Joined ZeroTier Network:', response.data);
  } catch (err) {
    console.error('Error joining ZeroTier Network:', err.response ? err.response.data : err.message);
  }
}

// Function to get ZeroTier network status
async function getNetworkStatus() {
  try {
    const response = await axios.get(
      `${zerotierBaseURL}/network/${networkId}`, 
      {
        headers: {
          Authorization: `Bearer ${zerotierToken}`,
        },
      }
    );
    return response.data;
  } catch (err) {
    console.error('Error getting ZeroTier network status:', err.response ? err.response.data : err.message);
  }
}

// Connect to MySQL using the ZeroTier IP
async function connectToMySQL(zerotierIP) {
  const connection = mysql.createConnection({
    host: zerotierIP, // ZeroTier IP address of the MySQL server
    user: 'root', // MySQL username
    password: '8717', // MySQL password
    database: 'dev_intra', // Database name
    port: 3311, // Default MySQL port
  });

  connection.connect((err) => {
    if (err) {
      console.error('Error connecting to MySQL:', err.stack);
      return;
    }
    console.log('Connected to MySQL as ID', connection.threadId);
  });

  // Sample query
  connection.query('SELECT * FROM your_table', (err, results) => {
    if (err) {
      console.error('Query error:', err.stack);
    } else {
      console.log('Results:', results);
    }
    
    connection.end();
  });
}

async function main() {
  // Join the ZeroTier network
  await joinNetwork();

  // Get ZeroTier network status
  const networkStatus = await getNetworkStatus();
  if (networkStatus && networkStatus.assignedAddresses.length > 0) {
    // Assuming the first assigned address is the private IP we need
    const zerotierIP = networkStatus.assignedAddresses[0];
    console.log('Using ZeroTier IP:', zerotierIP);

    // Connect to MySQL using the ZeroTier IP
    connectToMySQL(zerotierIP);
  } else {
    console.error('Failed to retrieve ZeroTier IP');
  }
}

main();
