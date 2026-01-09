from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, jsonify
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


# Create a simple view
class SimpleDagActionView(AppBuilderBaseView):
    default_view = "index"
    route_base = "/dagaction"

    @expose("/")
    def index(self):
        """Landing page."""
        return """
        <html>
        <head>
            <title>Custom DAG Action</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    max-width: 800px;
                    margin: 50px auto;
                    padding: 20px;
                }
                .action-box {
                    border: 2px solid #4CAF50;
                    border-radius: 8px;
                    padding: 20px;
                    margin: 20px 0;
                }
                button {
                    background-color: #4CAF50;
                    color: white;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 4px;
                    cursor: pointer;
                    font-size: 16px;
                }
                button:hover {
                    background-color: #45a049;
                }
                .result {
                    margin-top: 20px;
                    padding: 15px;
                    background-color: #f0f0f0;
                    border-radius: 4px;
                }
            </style>
        </head>
        <body>
            <h1>🚀 Custom DAG Action Tool</h1>
            
            <div class="action-box">
                <h2>Execute Custom Action</h2>
                <p>Enter a DAG ID to execute a custom action:</p>
                
                <input type="text" id="dagId" placeholder="Enter DAG ID" style="padding: 8px; width: 300px;">
                <button onclick="executeAction()">Execute Action</button>
                
                <div id="result" class="result" style="display:none;"></div>
            </div>

            <div class="action-box">
                <h2>Quick Links</h2>
                <ul>
                    <li><a href="/dags">View All DAGs</a></li>
                    <li><a href="/dagaction/test/my_dag">Test Action (example)</a></li>
                </ul>
            </div>

            <script>
                function executeAction() {
                    const dagId = document.getElementById('dagId').value;
                    if (!dagId) {
                        alert('Please enter a DAG ID');
                        return;
                    }
                    
                    fetch('/dagaction/test/' + dagId)
                        .then(response => response.json())
                        .then(data => {
                            const resultDiv = document.getElementById('result');
                            resultDiv.style.display = 'block';
                            resultDiv.innerHTML = '<strong>Result:</strong><br>' + 
                                                 'Status: ' + data.status + '<br>' +
                                                 'Message: ' + data.message;
                        })
                        .catch(error => {
                            alert('Error: ' + error);
                        });
                }
            </script>
        </body>
        </html>
        """

    @expose("/test/<dag_id>")
    def test_action(self, dag_id):
        """Execute test action for a DAG."""
        # Your custom logic here
        print(f"🎯 Custom action triggered for DAG: {dag_id}")
        
        # You can add any logic you want here:
        # - Send notifications
        # - Trigger external APIs
        # - Log to external systems
        # - etc.
        
        return jsonify({
            "status": "success",
            "message": f"Custom action completed for DAG: {dag_id}",
            "dag_id": dag_id,
            "action": "test_executed"
        })


# Create the view instance
v = SimpleDagActionView()


# Create the plugin
class SimpleDagActionPlugin(AirflowPlugin):
    name = "simple_dag_action"
    appbuilder_views = [
        {
            "name": "DAG Custom Action",
            "category": "Tools",
            "view": v
        }
    ]