/*
angular.module('divApp', [])
  .controller('DivendScoutController', function($scope, $http) {
    var divData = this;
*/
var app = angular.module('divApp', []);

app.controller('DivendScoutController', function($scope, $http) {
  divData = this;
  //$http.get("http://www.w3schools.com/angular/customers.php")
  $http.get("/data/SNP")
  //$http.get("SNP.json")
    .success(function(response) {
      //divData.names = response.records;
      //console.log(response);
      divData.stocks = response['records']
    })
    .error(function(data, status, headers, config) {
      console.log("Error");
      console.log("data: " + data);
      console.log("status: " + status);
      console.log("header: " + headers);
      console.log("config: " + config);
    });
   });

