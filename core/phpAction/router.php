<?php

$ACTION_SRC = 'action.php';

switch ($_SERVER["REQUEST_URI"]) {
    case "/init":
        $post_body = file_get_contents('php://input');
        $data = json_decode($post_body, true);
        file_put_contents($ACTION_SRC, $data["value"]["code"]);

        // Nothing to return.
        echo "OK\n";
        return true;

    case "/run":
        // Load action code.
        $action_code = file_get_contents($ACTION_SRC);

        ob_start();
        eval(' ?> ' . $action_code); // wtf
        ob_end_clean();

        // Load action params.
        $post_body = file_get_contents('php://input');
        $data = json_decode($post_body, true);

        // Run.
        $res = main($data["value"]);

        // Return.
        header('Content-Type: application/json');
        echo json_encode($res) . "\n";
        return true;

    default:
        return true;
}

?>
