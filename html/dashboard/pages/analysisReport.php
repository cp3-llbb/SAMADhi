        <!-- Page Content -->
            <div class="container-fluid">
                <div class="row">
                    <div class="col-lg-12">
			<h1 class="page-header">SAMADhi results - Database analysis report</h1>
                    </div>
                    <!-- /.col-lg-12 -->
                </div>
                <!-- /.row -->
                <div class="row">
                    <div class="col-lg-4 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Time profile
                            </div>
                            <div class="panel-body" id="timeProfileContainer">
                            </div>
                        </div>
		    </div>
                    <div class="col-lg-4 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Results Author
                            </div>
                            <div class="panel-body" id="authorsPlotContainer">
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-4 col-xs-12">
                        <div class="panel panel-primary">
                            <div class="panel-heading">
                                Number of sample(s) used
                            </div>
                            <div class="panel-body" id="samplesPlotContainer">
                            </div>
                        </div>
                    </div>
		</div>
            	<div class="row">
                    <div class="col-lg-12">
                        <div class="panel panel-primary" id="MissingDirSamples">
                            <div class="panel-heading">
                                Results with missing path <span class="badge" id="numberOfMissingDirSamples"></span>
                            </div>
                            <!-- .panel-heading -->
                            <div class="panel-body">
                                <div class="panel-group" id="accordionA">
		    		<p>The following results are pointing to non-existant locations on the server.</p>
                                </div>
                            </div>
                            <!-- .panel-body -->
                        </div>
                        <!-- /.panel -->
                    </div> <!-- /.col-lg-12 -->
		    <div class="col-lg-12 col-xs-12">
                        <div class="panel panel-primary" id="DbProblems">
                            <div class="panel-heading">
				Database Inconsistencies <span class="badge" id="numberOfDbProblems"></span>
                            </div>
                            <div class="panel-body">
                                <div class="panel-group" id="accordionB">
			            <p>The following results have inconsitencies in their relation to other entries.</p>
                                </div>
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
            <script src="dashboard/js/resultReport.js"></script>

