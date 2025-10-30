
CUSTOM_JS = """
<script>
function createReadmeModal() {
    // Remove existing modal if any
    const existingModal = document.getElementById('readmeModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    const modal = document.createElement('div');
    modal.id = 'readmeModal';
    modal.style.cssText = 'display: block; position: fixed; z-index: 10000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0, 0, 0, 0.7);';
    
    const modalContent = document.createElement('div');
    modalContent.style.cssText = 'background-color: #1f2937; margin: 2% auto; border-radius: 10px; width: 90%; max-width: 900px; max-height: 90vh; overflow-y: auto; position: relative; font-family: system-ui, sans-serif; line-height: 1.6; color: #f3f4f6; border: 1px solid #374151;';
    
    modalContent.innerHTML = `
    <span onclick="closeReadmeModal()" style="color: #9ca3af; float: right; font-size: 28px; font-weight: bold; position: absolute; right: 20px; top: 15px; cursor: pointer;">&times;</span>
    <div class="kafka-modal-content" style="background:#000;color:#fff;font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,sans-serif;line-height:1.6;padding:1rem;max-height:80vh;overflow-y:auto;">
  <style>
    :root {
      --bg: #000;
      --text: #fff;
      --muted: #ccc;
      --panel: #111;
      --panel-2: #222;
      --border: #333;
      --accent: #fff;
      --radius: 12px;
    }
    h1, h2, h3 { color: var(--text); }
    p, li { color: var(--muted); }
    .hero {
      text-align: center;
      margin-bottom: 1.5rem;
    }
    .card {
      background: var(--panel-2);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1rem 1.2rem;
      margin-bottom: 1rem;
    }
    .grid {
      display: grid;
      gap: 1rem;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }
    a { color: var(--accent); text-decoration: underline; }
    .cta {
      background: var(--text);
      color: var(--bg);
      padding: .6rem 1rem;
      border-radius: var(--radius);
      font-weight: bold;
      text-decoration: none;
      display: inline-block;
      margin-top: .5rem;
    }
    .divider { height: 1px; background: var(--border); margin: 1.5rem 0; }
  </style>

  <div class="container">
    <header class="hero">
      <h1>🚀 Kafka Broker Console!</h1>
    </header>

	<section>
	  <h3> About Kafka: </h3>
	  <div class="card">
		<p>This repository is my implemenatation of Kafka, an open-source, distributed event streaming platform. It combines messaging, storage, and stream processing to handle large volumes of real-time data.

- How it works: Producers send data to "topics" and consumers read data from these topics. Topics contain many partitions to isolate different kind of events in a Topic and for data replication.
- Common uses: Building real-time data pipelines, data integration, real-time analytics, and event-driven architectures.</p>
	</section>
    <section>
      <h3>📋 What is this app?</h3>
      <div class="card">
        <p>This Kafka Broker Console lets you manage topics, produce/consume messages, and monitor operations directly in your browser through an intuitive interface. Kafka Broker is already up and running in the backend!</p>
      </div>
    </section>

    <section>
      <h3>🎯 Key Components</h3>
      <div class="grid">
        <div>
            <div class="card">
            <h3>📊 Overview Tab</h3>
            <ul>
                <li>📈 View cluster statistics (total topics, partitions, records)</li>
                <li>📋 Browse topics with partition details</li>
                <li>🔄 Real-time refresh capabilities</li>
            </ul>
            </div>
            <div class="card">
            <h3>➕ Create Topics Tab</h3>
            <ul>
                <li>✏️ Add topics with custom configurations</li>
                <li>🎛️ Configure partitions and replication factors</li>
                <li>📝 Batch topic creation with validation</li>
                <li>❌ Remove topics from queue before creation</li>
            </ul>
            </div>
        </div>
        <div>
            <div class="card">
            <h3>📥 Consume Tab</h3>
            <ul>
                <li>📖 Read messages from any topic/partition</li>
                <li>⚡ Stream events in real time</li>
                <li>📍 Control offset positioning</li>
                <li>🔍 View message keys and values</li>
            </ul>
            </div>
            <div class="card">
            <h3>📡 API Activity Monitor</h3>
            <ul>
                <li>📜 Real-time API call logging</li>
                <li>🕐 Timestamp tracking</li>
                <li>📋 Detailed request/response information</li>
                <li>🔍 Click to view operation details</li>
            </ul>
            </div>
        </div>
      </div>
    </section>

    <section>
      <h2>📦 GitHub & Project Info</h2>
      <div class="grid">
        <div class="card">Check out the <a href="#">GitHub repo</a> 🎛️ for more information about the project. The repository includes both a Kafka broker and a Python Kafka client library to interact with your cluster programmatically. All data can be monitored in real time through this UI. \n You can set this up on your PC by following the setup instructions in the repository.</div>
        <div class="card">This Kafka implementation fully adheres to the Kafka wire protocol 📜, so the official Python Kafka library can communicate with the broker. You can even download Kafka data files and inspect them to learn more about Kafka internals 🔍</div>
      </div>
    </section>

    <div class="divider"></div>

    <footer>
      <p><strong>🚀 Ready to manage your Kafka cluster?</strong> <a class="cta" onclick="closeReadmeModal()">Open the UI</a> </p>
      
    </footer>
  </div>
</div>
        `;
    
    modal.appendChild(modalContent);
    document.body.appendChild(modal);
    
    // Close modal when clicking outside
    modal.onclick = function(event) {
        if (event.target === modal) {
            closeReadmeModal();
        }
    };
}

function closeReadmeModal() {
    const modal = document.getElementById('readmeModal');
    if (modal) {
        modal.remove();

        refreshButton = document.getElementById('refresh-button');
        if (refreshButton) {
            refreshButton.click();
        }
    }
}

// Show modal when page loads
function tryShowModal() {
    if (document.body) {
        createReadmeModal();
    } else {
        setTimeout(tryShowModal, 100);
    }
}

// Try multiple ways to ensure modal shows
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
        setTimeout(tryShowModal, 500);
    });
} else {
    setTimeout(tryShowModal, 500);
}

</script>
"""

WARNING_MESSAGE = """
        ### ⚠️ **WARNING: Cleanup Kafka Data**
        
        This action will **permanently delete** all files in:
        ```
        /tmp/kraft-combined-logs/*
        ```
        
        **This includes:**
        - All topic data
        - Partition logs  
        - Metadata files
        - Offset information
        
        **This action cannot be undone!**
        
        Are you sure you want to proceed?

"""