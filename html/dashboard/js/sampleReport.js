$(function(){
    //////////////////////////////////////////////////
    // read the json file to fill in the page content
    //////////////////////////////////////////////////
    $.getJSON( "../dashboard/data/SamplesAnalysisReport.json", function( data  ) {
        /////////////////////////////////////////////////
        // samples with missing directory
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
                                missingDirSamples[i]["name"]+" (created on "+missingDirSamples[i]["creation_time"]+" by "+missingDirSamples[i]["author"]+")"
                            )
                        )
                    )
                ).append(
                    $("<div />").addClass("panel-collapse collapse").attr({id: "collapseA"+i}).html(
                        $("<div />").addClass("panel-body").html(
                            $("<div />").addClass("table-responsive").html(
                                $("<div />").addClass("table table-hover").html(
                                    $("<tbody />").html($("<tr />").html("<td>Path</td><td>"+missingDirSamples[i]["path"]+"</td>")).append(
                                        $("<tr />").html("<td>Type</td><td>"+missingDirSamples[i]["sampletype"]+"</td>")).append(
                                        $("<tr />").html("<td>number of processed events</td><td>"+missingDirSamples[i]["nevents_processed"]+"</td>")).append(
                                        $("<tr />").html("<td>number of events</td><td>"+missingDirSamples[i]["nevents"]+"</td>")).append(
                                        $("<tr />").html("<td>(Effective) luminosity</td><td>"+missingDirSamples[i]["luminosity"]+"</td>")).append(
                                        $("<tr />").html("<td>Normalization</td><td>"+missingDirSamples[i]["normalization"]+"</td>")).append(
                                        $("<tr />").html("<td>Code version</td><td>"+missingDirSamples[i]["code_version"]+"</td>")).append(
                                        $("<tr />").html("<td>Comment</td><td>"+missingDirSamples[i]["user_comment"]+"</td>")).append(
                                        $("<tr />").html("<td>Source dataset</td><td>"+missingDirSamples[i]["source_dataset_id"]+"</td>")).append(
                                        $("<tr />").html("<td>Source sample</td><td>"+missingDirSamples[i]["source_sample_id"]+"</td>")
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
                                dbProblems[i][0]["name"]+" (created on "+dbProblems[i][0]["creation_time"]+" by "+dbProblems[i][0]["author"]+")  "
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
                                        $("<tr />").html("<td>Type</td><td>"+dbProblems[i][0]["sampletype"]+"</td>")).append(
                                        $("<tr />").html("<td>number of processed events</td><td>"+dbProblems[i][0]["nevents_processed"]+"</td>")).append(
                                        $("<tr />").html("<td>number of events</td><td>"+dbProblems[i][0]["nevents"]+"</td>")).append(
                                        $("<tr />").html("<td>(Effective) luminosity</td><td>"+dbProblems[i][0]["luminosity"]+"</td>")).append(
                                        $("<tr />").html("<td>Normalization</td><td>"+dbProblems[i][0]["normalization"]+"</td>")).append(
                                        $("<tr />").html("<td>Code version</td><td>"+dbProblems[i][0]["code_version"]+"</td>")).append(
                                        $("<tr />").html("<td>Comment</td><td>"+dbProblems[i][0]["user_comment"]+"</td>")).append(
                                        $("<tr />").html("<td>Source dataset</td><td>"+dbProblems[i][0]["source_dataset_id"]+"</td>")).append(
                                        $("<tr />").html("<td>Source sample</td><td>"+dbProblems[i][0]["source_sample_id"]+"</td>")
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
        var statistics = data["SampleStatistics"];
        // Number of events in samples
        var sampleNevents = statistics["sampleNevents"];
        $('#sampleNeventsContainer').highcharts({
            series: [{
                name: "Samples",
                data: sampleNevents
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
                    text:'Number of Samples',
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
        // Evolution of the number of events in samples
        var sampleNeventsTimeprof = statistics["sampleNeventsTimeprof"];
        $('#sampleNeventsTimeprofContainer').highcharts({
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
                type: 'logarithmic' ,
                title: {
                    text:'Number of Events',
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
                data: sampleNeventsTimeprof
            }]
        });
        // Number of events processed
        var sampleNeventsProcessed = statistics["sampleNeventsProcessed"];
        $('#sampleNeventsProcessedContainer').highcharts({
            series: [{
                name: "SAMADhi",
                data: sampleNeventsProcessed
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
                    text:'Number of Samples',
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
        // Evolution of the number of events processed
        var sampleNeventsProcessedTimeprof = statistics["sampleNeventsProcessedTimeprof"];
        $('#sampleNeventsProcessedTimeprofContainer').highcharts({
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
                type: 'logarithmic' ,
                title: {
                    text:'Number of Events',
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
                data: sampleNeventsProcessedTimeprof
            }]
        });
        // Time profile
        var samplesTimeprof = statistics["samplesTimeprof"];
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
                type: 'logarithmic' ,
                title: {
                    text:'Number of Samples',
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
                data: samplesTimeprof
            }]
        });
        // Sample Types
        var sampleTypes = statistics["sampleTypes"];
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
                    name: 'Samples share',
            data: sampleTypes
                }]
            });
        $("#typePlotContainer").siblings().filter(".panel-heading").html("Samples type")
        // Samples Author
        var sampleAuthors = statistics["sampleAuthors"];
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
                name: 'Samples share',
                data: sampleAuthors
            }]
        });
        $("#authorsPlotContainer").siblings().filter(".panel-heading").html("Samples author")
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
