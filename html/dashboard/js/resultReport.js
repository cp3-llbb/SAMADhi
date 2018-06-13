$(function(){
    //////////////////////////////////////////////////
    // read the json file to fill in the page content
    //////////////////////////////////////////////////
    $.getJSON( "../dashboard/data/ResultsAnalysisReport.json", function( data  ) {
        /////////////////////////////////////////////////
        // result with missing directory
        /////////////////////////////////////////////////
        var missingDirSamples = data["MissingDirSamples"];
        var missingDirSamples_pg = $("#MissingDirSamples").find(".panel-group");
        // set header for missing dir samples
        $("#numberOfMissingDirSamples").html(missingDirSamples.length);
        if(missingDirSamples.length==0) {
            $("#MissingDirSamples").addClass("hidden")
        }
        // fill missing dir samples
        for (i = 0; i < missingDirSamples.length; i++) { 
            missingDirSamples_pg.append(
                $("<div />").addClass("panel panel-info").append(
                    $("<div />").addClass("panel-heading").html(
                        $("<h4 />").addClass("panel-title").html(
                            $("<a />").attr({"data-toggle":"collapse", "data-parent":"#accordionA", "href":"#collapseA"+i}).html(
                                "Result #"+missingDirSamples[i]["result_id"]+" (created on "+missingDirSamples[i]["creation_time"]+" by "+missingDirSamples[i]["author"]+")"
                            )
                        )
                    )
                ).append(
                    $("<div />").addClass("panel-collapse collapse").attr({id: "collapseA"+i}).html(
                        $("<div />").addClass("panel-body").html(
                            $("<div />").addClass("table-responsive").html(
                                $("<div />").addClass("table table-hover").html(
                                    $("<tbody />").html($("<tr />").html("<td>Path</td><td>"+missingDirSamples[i]["path"]+"</td>")).append(
                                        $("<tr />").html("<td>Description</td><td>"+missingDirSamples[i]["description"]+"</td>")
                                    )
                                )
                            )
                        )
                    )
                )
            )

        }
        /////////////////////////////////////////////////
        // database inconsistencies
        /////////////////////////////////////////////////
        var dbProblems = data["DatabaseInconsistencies"];
        var dbProblems_pg = $("#DbProblems").find(".panel-group");
        // set header for db inconsistencies
        $("#numberOfDbProblems").html(dbProblems.length);
        if(dbProblems.length==0) {
            $("#DbProblems").addClass("hidden");
        }
        // fill db inconsistencies
        for (i = 0; i < dbProblems.length; i++) {
            dbProblems_pg.append(
                $("<div />").addClass("panel panel-info").append(
                    $("<div />").addClass("panel-heading").html(
                        $("<h4 />").addClass("panel-title").html(
                            $("<a />").attr({"data-toggle":"collapse", "data-parent":"#accordionB", "href":"#collapseB"+i}).html(
                                "Result #"+dbProblems[i][0]["result_id"]+" (created on "+dbProblems[i][0]["creation_time"]+" by "+dbProblems[i][0]["author"]+")  "
                            )
                        ).append(
                            $("<span />").addClass("label label-danger").html( 
                                $("<strong />").html(dbProblems[i][1])
                            )
                        )
                    )
                ).append(
                    $("<div />").addClass("panel-collapse collapse").attr({id: "collapseB"+i}).html(
                        $("<div />").addClass("panel-body").html(
                            $("<div />").addClass("table-responsive").html(
                                $("<div />").addClass("table table-hover").html(
                                    $("<tbody />").html($("<tr />").html("<td>Path</td><td>"+dbProblems[i][0]["path"]+"</td>")).append(
                                        $("<tr />").html("<td>Description</td><td>"+dbProblems[i][0]["description"]+"</td>")
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
        var statistics = data["ResultsStatistics"];
        // Time profile
        var resultsTimeprof = statistics["resultsTimeprof"];
        $('#timeProfileContainer').highcharts({
            chart: {
                type: 'line'
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
                title: {
                    text:'Number of Results',
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
                data: resultsTimeprof
            }]
        });
        // Author
        var resultsAuthors = statistics["resultsAuthors"];
        $('#authorsPlotContainer').highcharts({
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
                    size: '85%',
                    allowPointSelect: true,
                    cursor: 'pointer',
                    depth: 35,
                    dataLabels: {
                        enabled: true,
                        format: '{point.name}'
                    }
                }
            },
            series: [{
                type: 'pie',
                name: 'Result share',
                data: resultsAuthors
            }]
        });
        $("#authorsPlotContainer").siblings().filter(".panel-heading").html("Results author")
        var resultsSamples = statistics["resultNsamples"];
        $('#samplesPlotContainer').highcharts({
            series: [{
                name: "Samples",
                data: resultsSamples
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
                    text:'Number of samples',
                    align: 'high'
                }
            },
            yAxis: { 
                title: {
                    text:'Number of results',
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
