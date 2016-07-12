request = require("request");

/* Uses the API at randomuser.me to get a random name. */
function main(args) {
    return new Promise(function(resolve, reject) {
        request({
            method: "GET",
            uri:    "https://randomuser.me/api/",
            json:   true
        }, function (error, response, body) {
            if(error) {
                reject({ "requestFailed": true });
            } else {
                var person = body["results"][0];

                resolve({ "name" : person["name"]["first"] + " " + person["name"]["last"] });
            }
        });
    });
}
