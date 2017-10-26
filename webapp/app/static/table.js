var table;

function initialize(){
	console.log("IM IN");
	table = new webix.ui({
	    view:"datatable",
	    container:"business-table",
	    datatype: "json",
	    columns: {id: "bussiness_id", header: "Traffic Surges", height: 100, width: 180},
	    data: [{"id": 1, "bussiness_id": 'foo'}, {"id": 2, "bussiness_id": 'bar'}]
	});
}

$(document).ready(initialize);