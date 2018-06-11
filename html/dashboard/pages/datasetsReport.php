            <div class="container-fluid">
                <div class="row">
                    <div class="col-lg-12">
			<h1 class="page-header">SAMADhi datasets - Database analysis report</h1>
                    </div>
                    <!-- /.col-lg-12 -->
                </div>
                <!-- /.row -->
                <div class="row">
                    <div class="col-lg-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Time profile
                            </div>
                            <div class="panel-body" id="timeProfileContainer">
                            </div>
                        </div>
		    </div>
                    <div class="col-lg-6 col-sm-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Dataset Globaltag
                            </div>
                            <div class="panel-body" id="gtPlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 col-sm-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Dataset Type
                            </div>
                            <div class="panel-body" id="typePlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 col-sm-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                CMSSW release
                            </div>
                            <div class="panel-body" id="releasePlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 col-sm-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                CM Energy
                            </div>
                            <div class="panel-body" id="energyPlotContainer">
                            </div>
                        </div>
                    </div>
            </div><div class="row">
                <div class="col-lg-12">
                    <div class="panel panel-danger" id="WrongDatasets">
                        <div class="panel-heading">
                            Datasets with DAS inconsistencies <span class="badge" id="numberOfWrongDatasets"></span>
                        </div>
                        <!-- .panel-heading -->
                        <div class="panel-body">
                            <div class="panel-group" id="accordionA">
				<p>The following datasets are not consistent with DAS.</p>
                            </div>
                        </div>
                        <!-- .panel-body -->
                    </div>
                    <!-- /.panel -->
                </div>
                <div class="col-lg-12">
                    <div class="panel panel-warning" id="OrphanDatasets">
                        <div class="panel-heading">
                            Orphan datasets <span class="badge" id="numberOfOrphanDatasets"></span>
                        </div>
                        <!-- .panel-heading -->
                        <div class="panel-body">
                            <div class="panel-group" id="accordionB">
				<p>The following datasets do not have any derived sample.</p>
                            </div>
                        </div>
                        <!-- .panel-body -->
                    </div>
                    <!-- /.panel -->
                </div>
                    <div class="col-lg-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Number of events in the dataset
                            </div>
                            <div class="panel-body" id="datasetNeventsPlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Size of the dataset
                            </div>
                            <div class="panel-body" id="datasetSizePlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Number of derived samples
                            </div>
                            <div class="panel-body" id="datasetSamplesPlotContainer">
                            </div>
                        </div>
                    </div>
                </div>
                <!-- /.row -->
            </div>
            <!-- /.container-fluid -->
    <script src="dashboard/vendor/jquery/jquery.min.js"></script>
    <script src="dashboard/vendor/bootstrap/js/bootstrap.min.js"></script>
    <script src="dashboard/vendor/metisMenu/metisMenu.min.js"></script>
    <script src="dashboard/vendor/highcharts/highcharts.js"></script>
    <script src="dashboard/vendor/highcharts/highcharts-3d.js"></script>
    <script src="dashboard/vendor/highcharts/themes/grid-light.js"></script>
    <script src="dashboard/js/webAccess.js"></script>
    <script src="dashboard/vendor/highcharts/modules/drilldown.js"></script>
    <script src="dashboard/js/datasetsReport.js"></script>

