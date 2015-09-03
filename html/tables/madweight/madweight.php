<?php
class tables_madweight {
    function valuelist__isrdefs() {
      return array(0=>'No Correction', 1=>'Correction by Pt(ISR)', 2=>'Correction by Pt(ISR) and boost to rest', 3=>'Unconstrained Pt(ISR)');
    }
    function valuelist__nwadefs() {
      return array(0=>'Disabled', 1=>'Enabled');
    }
    function getDescription(&$record) {
      return $record->val('diagram');
    }
}
?>
