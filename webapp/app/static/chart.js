// var business_data = JSON.parse('{{maps_data | safe}}');
// var traffic_data = JSON.parse('{{traffic_data | safe}}');
var myChart;

function display_timeseries(b_id, name){
	var time_series_data = $.getJSON('/get_realtime_traffic', function(data){
		var display_data = data['traffic_data'][b_id];
		console.log(display_data['visits'].length, '*******')
		var time_series_avg = data['time_series_avg'];
		if (display_data !== undefined || display_data === {}){
			myChart = Highcharts.chart('chart_container', {
				chart: {
					type: 'line',
					zoomType: 'x'
				},
				title: {
					text: name + "'s Web Traffic"
				},
				xAxis: {
					categories: display_data['times']
				},
				yAxis: {
					title: {
						text: "Visits"
					}
				},
				series: [
				{
					name: name + "'s Web Traffic",
					data: display_data['visits']
				},
				{
					name: "Average",
					data: time_series_avg
				},]
			});
		} else {
			console.log("SOMETHING WRONG WITH CACHE DATA: ", display_data);
		}
	});
}