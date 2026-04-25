function sendEmail(type, trackingNumber) {
    let subject = '';
    let body = '';

    switch(type) {
        case 'delay':
            subject = 'Delay in Delivery';
            body = `Dear Team,\n\nPlease note that there has been significant delay in delivery of CN# ${trackingNumber}, Please arrange urgent delivery.\n\nThanks and Regards,\nTick Bags\nwww.tickbags.com`;
            break;
        case 'incomplete':
            subject = 'FAKE DELIVERY REASON';
            body = `Dear Team,\n\nPlease note that consignee is waiting for delivery against CN# ${trackingNumber}, while the tracking status says incomplete address even though the address is 100% accurate and phone number is completely accessible. Please arrange urgent delivery and make sure this shipment is not returned.\n\nThanks and Regards,\nTick Bags\nwww.tickbags.com`;
            break;
        case 'refusal':
            subject = 'URGENT - FAKE ORDER REFUSAL';
            body = `Dear Team,\n\nPlease note that consignee is waiting for delivery against CN# ${trackingNumber}, while the tracking status says that customer has refused the delivery. Please arrange urgent delivery and make sure this shipment is not returned.\n\nThanks and Regards,\nTick Bags\nwww.tickbags.com`;
            break;
    }

    fetch('/send-email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            to: ['Cod.lhe@leopardscourier.com','Cod.lhe1@leopardscourier.com','Cod.lhe5@leopardscourier.com','Codproject.lhe@leopardscourier.com'],
            cc: ['ahmad.aslam@leopardscourier.com','muneeb.shahzad@hotmail.com'],
            subject, body
        }),
    })
    .then(response => {
        alert(response.ok ? 'Email sent successfully' : 'Failed to send email');
    })
    .catch(() => alert('Error sending email'));
}

function toggleStatusButtons(button) {
    var statusButtons = button.nextElementSibling;
    statusButtons.style.display = (statusButtons.style.display === 'block') ? 'none' : 'block';
}

function applyTag(orderId, tag) {
    return fetch('/apply_tag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ order_id: orderId, tag: tag })
    })
    .then(response => response.json())
    .then(data => {
        if (!data.success) {
            console.error('Failed to apply tag:', data.error);
        }
        return data;
    })
    .catch(error => {
        console.error('Error applying tag:', error);
    });
}

document.addEventListener('DOMContentLoaded', function () {
    var refreshBtn = document.getElementById('refreshButton');
    if (!refreshBtn) return;

    refreshBtn.addEventListener('click', async function () {
        refreshBtn.disabled = true;
        refreshBtn.textContent = 'Refreshing...';
        try {
            const response = await fetch('/refresh', { method: 'POST' });
            const result = await response.json();
            if (result.message === 'Data refreshed successfully') {
                location.reload();
            } else {
                alert('Failed to refresh data: ' + (result.message || 'Unknown error'));
            }
        } catch (e) {
            alert('Failed to refresh data. Check connection.');
        } finally {
            refreshBtn.disabled = false;
            refreshBtn.textContent = 'Refresh Data';
        }
    });
});
