'use strict';
var request = require('request');
var AWS = require('aws-sdk');

AWS.config.update({
    region: "us-west-1"
});

var docClient = new AWS.DynamoDB.DocumentClient();
var table = "BroncoExpress";
var busIDs = ["1706", "2772", "2561", "1705", "1722", "1707"];

module.exports.hello = (event, context, callback) => {
  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Go Serverless v1.0! Your function executed successfully!',
      input: event,
    }),
  };

  callback(null, response);

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // callback(null, { message: 'Go Serverless v1.0! Your function executed successfully!', event });
};

module.exports.fetch = (event, context, callback) => {
    fetchBusInfo();
    const response = {
        statusCode: 200,
        body: JSON.stringify({
            message: 'OK: Bus Info Fetched!',
            input: event,
        }),
    }

    callback(null, response);
}

module.exports.query = (event, context, callback) => {
    queryBusTimes(event, callback);
}

function fetchBusInfo() {
    request('https://rqato4w151.execute-api.us-west-1.amazonaws.com/dev/info', function (error, response, body) {
        if (!error && response.statusCode == 200) {
            console.log(body);
            var items = JSON.parse(body);
            for(var i = 0; i < items.length; i++) {
                console.log(items[i].id, items[i].logo, items[i].lat, items[i].lng, items[i].route);
                putItem(items[i].id.toString(), items[i].logo, items[i].lat, items[i].lng, items[i].route);
            }
        }
    })
}

function queryBusTimes(event, callback) {
    var recentBusTimes = [];
    var counter = 0;
    for(var i = 0; i < busIDs.length; ++i) {
        var params = {
            TableName : table,
            KeyConditionExpression: "#key = :inputName",
            ExpressionAttributeNames:{
                "#key": "id"
            },
            ExpressionAttributeValues: {
                ":inputName":busIDs[i]
            }
        };

        docClient.query(params, function(err, data) {
            if (err) {
                console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
            } else {
                console.log("Query succeeded.");
                delete data.Items[0].timestamp;
                recentBusTimes[counter] = data.Items[0];
                for(var j = 1; j < data.Items.length; ++j) {
                    if(data.Items[j].timestamp > recentBusTimes[counter].timestamp){
                        delete data.Items[j].timestamp;
                        recentBusTimes[counter] = data.Items[j];
                    }
                }

                counter++;
                console.log(counter);
                if(counter >= busIDs.length) {
                    console.log(recentBusTimes);

                    const response = {
                        statusCode: 200,
                        body: JSON.stringify({
                            message: recentBusTimes,
                            input: event
                        })
                    };

                    callback(null, response);
                }
            }
        });
    }
}

function putItem(busID, logo, lat, lng, route) {
    var params = {
        TableName:table,
        Item:{
            "id": busID,
            "timestamp": Date.now(),
            "logo": logo,
            "lat": lat,
            "lng": lng,
            "route": route
        }
    };

    console.log("Adding a new item...");
    docClient.put(params, function(err, data) {
        if (err) {
            console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Added item:", JSON.stringify(data, null, 2));
        }
    });
}