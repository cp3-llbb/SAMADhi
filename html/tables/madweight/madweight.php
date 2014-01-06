<?php
class tables_madweight {
    function valuelist__isrdefs() {
      return array(0=>'Ignore effect', 1=>'by Pt(ISR)', 2=>'by Pt(ISR) + boost to CMS frame', 3=>'???');
    }
    function valuelist__nwadefs() {
      return array(0=>'Disabled', 1=>'Enabled');
    }
    function getDescription(&$record) {
      return $record->val('diagram');
    }
}
?>
