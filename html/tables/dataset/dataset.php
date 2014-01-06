<?php
class tables_dataset {
    function getDescription(&$record) {
      return 'Process: '.$record->val('process');
    }
}
?>
