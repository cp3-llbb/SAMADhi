$(function(){
    // CREATE A FADE-IN METHOD FOR 3D-ARCS
    Highcharts.wrap(Highcharts.SVGRenderer.prototype, 'arc3d', function (proceed) {
        var result = proceed.apply(this, [].slice.call(arguments, 1));
        result.fadeIn = result.show;
        return result;
    });
    //////////////////////////////////////////////////
    // read the json file to fill in the page content
    //////////////////////////////////////////////////
    $.getJSON( "../dashboard/data/DatasetsAnalysisReport.json", function( data  ) {
        /////////////////////////////////////////////////
        // database inconsistencies
        /////////////////////////////////////////////////
        var dbProblems = data["DatabaseInconsistencies"].concat(data["IncompleteData"])
        var dbProblems_pg = $("#WrongDatasets").find(".panel-group");
        // set header for db inconsistencies
        $("#numberOfWrongDatasets").html(dbProblems.length);
        if(dbProblems.length==0) {
            $("#WrongDatasets").addClass("hidden");
        }
        // fill db inconsistencies
        for (i = 0; i < dbProblems.length; i++) {
            dbProblems_pg.append(
                $("<div />").addClass("panel panel-info").append(
                    $("<div />").addClass("panel-heading").html(
                        $("<h4 />").addClass("panel-title").html(
                            $("<a />").attr({"data-toggle":"collapse", "data-parent":"#accordionA", "href":"#collapseA"+i}).html(
                                dbProblems[i][0]["name"]+" (imported on "+dbProblems[i][0]["creation_time"]+")  "
                            )
                        ).append(
                            $("<span />").addClass("label label-danger").html( 
                                $("<strong />").html(dbProblems[i][1])
                            )
                        )
                    )
                ).append(
                    $("<div />").addClass("panel-collapse collapse").attr({id: "collapseA"+i}).html(
                        $("<div />").addClass("panel-body").html(
                            $("<div />").addClass("table-responsive").html(
                                $("<div />").addClass("table table-hover").html(
                                    $("<tbody />").html($("<tr />").html("<td>Process</td><td>"+dbProblems[i][0]["process"]+"</td>")).append(
                                        $("<tr />").html("<td>Type</td><td>"+dbProblems[i][0]["datatype"]+"</td>")).append(
                                        $("<tr />").html("<td>Number of events</td><td>"+dbProblems[i][0]["nevents"]+"</td>")).append(
                                        $("<tr />").html("<td>Size</td><td>"+dbProblems[i][0]["dsize"]+"</td>")).append(
                                        $("<tr />").html("<td>Cross-section</td><td>"+dbProblems[i][0]["xsection"]+"</td>")).append(
                                        $("<tr />").html("<td>CMSSW release</td><td>"+dbProblems[i][0]["cmssw_release"]+"</td>")).append(
                                        $("<tr />").html("<td>Globaltag</td><td>"+dbProblems[i][0]["globaltag"]+"</td>")).append(
                                        $("<tr />").html("<td>Energy</td><td>"+dbProblems[i][0]["energy"]+"</td>")).append(
                                        $("<tr />").html("<td>Comment</td><td>"+dbProblems[i][0]["user_comment"]+"</td>")).append(
                                    )
                                )
                            )
                        )
                    )
                )
            )
        }
        /////////////////////////////////////////////////
        // orphan nodes
        /////////////////////////////////////////////////
        var orphans = data["Orphans"]
        var orphans_pg = $("#OrphanDatasets").find(".panel-group");
        // set header for db inconsistencies
        $("#numberOfOrphanDatasets").html(orphans.length);
        if(orphans.length==0) {
            $("#OrphanDatasets").addClass("hidden");
        }
        // fill orphans
        for (i = 0; i < orphans.length; i++) {
            orphans_pg.append(
                $("<div />").addClass("panel panel-info").append(
                    $("<div />").addClass("panel-heading").html(
                        $("<h4 />").addClass("panel-title").html(
                            $("<a />").attr({"data-toggle":"collapse", "data-parent":"#accordionB", "href":"#collapseB"+i}).html(
                                orphans[i]["name"]+" (imported on "+orphans[i]["creation_time"]+")  "
                            )
                        )
                    )
                ).append(
                    $("<div />").addClass("panel-collapse collapse").attr({id: "collapseB"+i}).html(
                        $("<div />").addClass("panel-body").html(
                            $("<div />").addClass("table-responsive").html(
                                $("<div />").addClass("table table-hover").html(
                                    $("<tbody />").html($("<tr />").html("<td>Process</td><td>"+orphans[i]["process"]+"</td>")).append(
                                        $("<tr />").html("<td>Type</td><td>"+orphans[i]["datatype"]+"</td>")).append(
                                        $("<tr />").html("<td>Number of events</td><td>"+orphans[i]["nevents"]+"</td>")).append(
                                        $("<tr />").html("<td>Size</td><td>"+orphans[i]["dsize"]+"</td>")).append(
                                        $("<tr />").html("<td>Cross-section</td><td>"+orphans[i]["xsection"]+"</td>")).append(
                                        $("<tr />").html("<td>CMSSW release</td><td>"+orphans[i]["cmssw_release"]+"</td>")).append(
                                        $("<tr />").html("<td>Globaltag</td><td>"+orphans[i]["globaltag"]+"</td>")).append(
                                        $("<tr />").html("<td>Energy</td><td>"+orphans[i]["energy"]+"</td>")).append(
                                        $("<tr />").html("<td>Comment</td><td>"+orphans[i]["user_comment"]+"</td>")).append(
                                    )
                                )
                            )
                        )
                    )
                )
            )
        }
        /////////////////////////////////////////////////
        // Plots with db statistics
        /////////////////////////////////////////////////
        var statistics = data["DatasetsStatistics"];
	// time profile
        var datasetsTimeprof = statistics["datasetsTimeprof"];
        $('#timeProfileContainer').highcharts({
            chart: {
                type: 'spline'
            },
            title: {
                text: ''
            },
            legend: {
                enabled: false
            },
            xAxis: { 
                type: 'datetime',
                title: {
                    text:'Date',
                    align: 'high'
                }
            },
            yAxis: { 
		min: 0,
                title: {
                    text:'Number of Datasets',
                    align: 'high'
                }
            },
            plotOptions: {
                spline: {
                    marker: {
                        enabled: true
                    }
                }
            },
            series: [{
                name: "SAMADhi",
                data: datasetsTimeprof
            }]
        });
	// globaltag
        var globaltag = statistics["globaltag"];
            $('#gtPlotContainer').highcharts({
                chart: {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                },
                title: {
                    text: ''
                },
                tooltip: {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                },
                plotOptions: {
                    pie: {
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: false,
                            format: '{point.name}'
                        },
			showInLegend: true
                    }
                },
		legend: {
                	enabled: true,
	                layout: 'horizontal',
        	        align: 'center',
                	width: 200,
	                verticalAlign: 'bottom',
        	        useHTML: true,
            		},
                series: [{
                    type: 'pie',
                    name: 'Globaltag',
            data: globaltag
                }]
            });
	// type
        var datatype = statistics["datatype"];
            $('#typePlotContainer').highcharts({
                chart: {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                },
                title: {
                    text: ''
                },
                tooltip: {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                },
                plotOptions: {
                    pie: {
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: false,
                            format: '{point.name}'
                        },
			showInLegend: true
                    }
                },
                series: [{
                    type: 'pie',
                    name: 'Type',
            data: datatype
                }]
            });
	// release
        var cmssw_release = statistics["cmssw_release"];
	var cmssw_release_pattern = /(CMSSW_(\d*)_(\d*)_(\d*))_?(.*)/;
	var m;
	var cmssw_drilldown = {};
	var cmssw_data = [];
	cmssw_drilldown.series = [];
	cmssw_release.forEach(function(entry){
		if ((m = cmssw_release_pattern.exec(entry[0])) !== null) {
			var release = m[2]+"."+m[3]+"."+m[4];
			var result = $.grep(cmssw_drilldown.series, function(e){ return e.id == release; });
			if(result.length==0) {
				var drilldown_entry = {};
				drilldown_entry.name = release;
				drilldown_entry.id = release;
				drilldown_entry.data = [[m[5],entry[1]]];
				cmssw_drilldown.series.push(drilldown_entry);
			} else if(result.length==1) {
				result[0].data.push([m[5],entry[1]]);
				
			} else {
				console.log("Error: two entries with same id");
			}
		}
	});
        cmssw_drilldown.series.forEach(function(entry){
		var cmssw_data_entry = { name: entry.name, drilldown: entry.id, y:0};
                entry.data.forEach(function(dataentry){
			cmssw_data_entry.y += dataentry[1];
                });
		cmssw_data.push(cmssw_data_entry);
        });
	var releaseChart = {
                chart: {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                },
                title: {
                    text: ''
                },
                tooltip: {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                },
                plotOptions: {
                    pie: {
                        //size: '85%',
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: false,
                            format: '{point.name}'
                        },
			showInLegend: true
                    }
                },
                series: [{
                    type: 'pie',
                    name: 'Release',
                    data: cmssw_data
                }],
	        drilldown: cmssw_drilldown
            };
	$('#releasePlotContainer').highcharts(releaseChart);
	// energy
        var energy = statistics["energy"];
        energy.forEach(function(entry){ entry[0] = String(entry[0])+" TeV"; })
            $('#energyPlotContainer').highcharts({
                chart: {
                    type: 'pie',
                    options3d: {
                        enabled: true,
                        alpha: 45,
                        beta: 0
                    }
                },
                title: {
                    text: ''
                },
                tooltip: {
                    pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
                },
                plotOptions: {
                    pie: {
                        //size: '85%',
                        allowPointSelect: true,
                        cursor: 'pointer',
                        depth: 35,
                        dataLabels: {
                            enabled: false,
                            format: '{point.name}'
                        },
			showInLegend: true
                    }
                },
                series: [{
                    //type: 'pie',
                    name: 'Energy',
		    colorByPoint: true,
                    data: energy
                }]
            });
        // Number of events in datasets
        var datasetsNevents = statistics["datasetsNevents"];
        $('#datasetNeventsPlotContainer').highcharts({
            series: [{
                name: "Nevents",
                data: datasetsNevents
            }],
            chart: {
                type: 'column'
            },
            title: {
                text: ''
            },
            legend: {
                enabled: false
            },
            xAxis: { 
                title: {
                    text:'Number of Events',
                    align: 'high'
                }
            },
            yAxis: { 
                type: 'logarithmic' ,
                title: {
                    text:'Number of Datasets',
                    align: 'high'
                }
            },
            plotOptions: {
                column: {
                    pointPadding: 0,
                    borderWidth: 0,
                    groupPadding: 0,
                    shadow: false
                }
            }
        });
	// D size
        var datasetsDsize = statistics["datasetsDsize"];
        $('#datasetSizePlotContainer').highcharts({
            series: [{
                name: "DSize",
                data: datasetsDsize
            }],
            chart: {
                type: 'column'
            },
            title: {
                text: ''
            },
            legend: {
                enabled: false
            },
            xAxis: { 
                title: {
                    text:'Size',
                    align: 'high'
                }
            },
            yAxis: { 
                type: 'logarithmic' ,
                title: {
                    text:'Number of Datasets',
                    align: 'high'
                }
            },
            plotOptions: {
                column: {
                    pointPadding: 0,
                    borderWidth: 0,
                    groupPadding: 0,
                    shadow: false
                }
            }
        });
	// Number of samples
        var datasetsNsamples = statistics["datasetsNsamples"];
        $('#datasetSamplesPlotContainer').highcharts({
            series: [{
                name: "Nsamples",
                data: datasetsNsamples
            }],
            chart: {
                type: 'column'
            },
            title: {
                text: ''
            },
            legend: {
                enabled: false
            },
            xAxis: { 
                title: {
                    text:'Samples',
                    align: 'high'
                }
            },
            yAxis: { 
                type: 'logarithmic' ,
                title: {
                    text:'Number of Datasets',
                    align: 'high'
                }
            },
            plotOptions: {
                column: {
                    pointPadding: 0,
                    borderWidth: 0,
                    groupPadding: 0,
                    shadow: false
                }
            }
        });
    } );
    // radial gradiant
    Highcharts.getOptions().colors = Highcharts.map(Highcharts.getOptions().colors, function (color) {
        return {
            radialGradient: {
                cx: 0.5,
                cy: 0.3,
                r: 0.7
            },
            stops: [
                [0, color],
                [1, Highcharts.Color(color).brighten(-0.3).get('rgb')] // darken
            ]
        };
    });
});
