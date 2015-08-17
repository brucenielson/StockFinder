
var app = angular.module('divApp', ['ngRoute']);

// https://docs.angularjs.org/api/ngRoute/service/$route#example
app.config(['$routeProvider',
  function ($routeProvider) {
    //http://stackoverflow.com/questions/17967437/angularjs-calls-http-multiple-times-in-controller
    $routeProvider
        .when("/", {
            templateUrl: "SNPList.html"})
        .when('/SNP', {
            templateUrl: 'SNPList.html'})
        .when('/details/:id', {
            templateUrl: 'StockDetail.html'})
        .otherwise({
            redirectTo: '/'
        });

  }]);


app.controller('DividendScoutController', function($scope, $http, $location) {
  divData = this;
  $http.get("/data/SNP")
    .success(function(response) {
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

  $scope.singleClick = function (stock_id) {
    //window.alert("/detail/"+stock_id);
    //go("/detail/"+stock_id)
    $location.path( "details/"+stock_id );
  }

  $scope.go = function ( path ) {
    $location.path( path );
  };

 });


app.controller('StockDetailController', function($scope, $http, $location, $routeParams) {
  detailData = this;
  detailData.id = $routeParams.id;
});
