const ngrok = require("ngrok"); ngrok.connect(3001).then(url => console.log("Tunnel URL:", url)).catch(err => console.error("Error:", err.message));
