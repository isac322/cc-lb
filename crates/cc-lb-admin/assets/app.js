async function fetchHealth() {
    try {
        const res = await fetch('/admin/health');
        const data = await res.json();
        
        const container = document.getElementById('health-status');
        container.innerHTML = '';
        
        const createP = (label, value) => {
            const p = document.createElement('p');
            const strong = document.createElement('strong');
            strong.textContent = label;
            p.appendChild(strong);
            p.appendChild(document.createTextNode(' ' + value));
            return p;
        };
        
        container.appendChild(createP('Status:', data.status));
        container.appendChild(createP('Version:', data.version));
        container.appendChild(createP('Git SHA:', data.git_sha));
        container.appendChild(createP('Uptime:', data.uptime_secs + 's'));
    } catch (e) {
        document.getElementById('health-status').textContent = 'Error loading health status';
    }
}

async function fetchPrincipals() {
    try {
        const token = localStorage.getItem('admin_token') || prompt('Enter admin token:');
        if (!token) return;
        localStorage.setItem('admin_token', token);

        const res = await fetch('/admin/principals', {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (res.status === 401) {
            localStorage.removeItem('admin_token');
            const tbody = document.querySelector('#principals-table tbody');
            tbody.innerHTML = '';
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = 4;
            td.textContent = 'Unauthorized. Refresh to enter token.';
            tr.appendChild(td);
            tbody.appendChild(tr);
            return;
        }

        const data = await res.json();
        const tbody = document.querySelector('#principals-table tbody');
        tbody.innerHTML = '';

        for (const p of data.principals) {
            const qRes = await fetch(`/admin/principals/${p.id}/quota`, {
                headers: { 'Authorization': `Bearer ${token}` }
            });
            const qData = await qRes.json();
            
            const tr = document.createElement('tr');
            
            const tdId = document.createElement('td');
            tdId.textContent = p.id;
            tr.appendChild(tdId);
            
            const tdReq = document.createElement('td');
            tdReq.textContent = `${qData.requests} / ${qData.requests_per_window}`;
            tr.appendChild(tdReq);
            
            const tdIn = document.createElement('td');
            tdIn.textContent = `${qData.input_tokens} / ${qData.input_tokens_per_window}`;
            tr.appendChild(tdIn);
            
            const tdOut = document.createElement('td');
            tdOut.textContent = `${qData.output_tokens} / ${qData.output_tokens_per_window}`;
            tr.appendChild(tdOut);
            
            tbody.appendChild(tr);
        }
    } catch (e) {
        const tbody = document.querySelector('#principals-table tbody');
        tbody.innerHTML = '';
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 4;
        td.textContent = 'Error loading principals';
        tr.appendChild(td);
        tbody.appendChild(tr);
    }
}

async function refresh() {
    await fetchHealth();
    await fetchPrincipals();
}

refresh();
setInterval(refresh, 5000);
