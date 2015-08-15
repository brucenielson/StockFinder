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


  $scope.set_payout_color = function (payout_ratio) {
    if (payout_ratio > 0.60) {
      return { color: "red" }
    }
   }

  $scope.set_target_color = function (percent_to_target) {

    if (percent_to_target === null) {
      return { background: "white" }
    }
    else if (percent_to_target == 0.0) {
      return { background: "green" }
    }
    else if (percent_to_target <= 0.05) {
      return { background: "lightgreen" }
    }
    else if (percent_to_target <= 0.1) {
      return { background: "greenyellow" }
    }
    else if (percent_to_target <= 0.2) {
      return { background: "yellow" }
    }
  }


 });
