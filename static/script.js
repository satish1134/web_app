function createDagLink(dagName) {
    const baseUrl = 'https://utc.well.com/sum/';
    const dagUrl = `${baseUrl}${dagName}/graph`;
    
    const link = document.createElement('a');
    link.href = dagUrl;
    link.textContent = dagName;
    link.target = '_blank'; // Opens in new tab
    link.rel = 'noopener noreferrer'; // Security best practice for external links
    return link;
}

function createStatusBadge(status, container) {
    if (!status) return null;
    
    const statusBadge = document.createElement('span');
    statusBadge.className = `badge badge-${getStatusBadgeClass(status)}`;
    statusBadge.textContent = status;
    
    if (container) {
        container.appendChild(statusBadge);
    }
    
    return statusBadge;
}

function createSanitizedElement(text) {
    const element = document.createElement('div');
    element.textContent = text; // textContent automatically escapes HTML
    return element.textContent;
}
function formatToEST(timestamp) {
    if (!timestamp) return '';
    
    try {
        const formattedDate = moment(timestamp);
        
        // Check if the date is valid
        if (!formattedDate.isValid()) {
            Logger.warn(`Invalid timestamp received: ${timestamp}`);
            return 'Invalid Date';
        }
        
        return formattedDate
            .tz('America/New_York')
            .format('YYYY-MM-DD HH:mm:ss');
    } catch (error) {
        Logger.error('Error formatting timestamp', error);
        return 'Invalid Date';
    }
}

function sanitizeHTML(str) {
    if (str === null || str === undefined) {
        return '';
    }
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// URL validation function
function isValidUrl(url) {
    try {
        const parsedUrl = new URL(url);
        const currentDomain = window.location.hostname;
        return parsedUrl.hostname === currentDomain;
    } catch (e) {
        return false;
    }
}

// Global variables
let loadingIndicator;
let DEBUG = false;

// Logging utility
const Logger = {
    error: function(message, error) {
        console.error(`${message}:`, error);
    },
    warn: function(message) {
        console.warn(message);
    },
    info: function(message) {
        console.info(message);
    }
};


// Session storage management
const SessionManager = {
    save: (key, value) => {
        try {
            sessionStorage.setItem(key, JSON.stringify(value));
        } catch (error) {
            Logger.error(`Failed to save to session storage: ${key}`, error);
        }
    },
    
    get: (key) => {
        try {
            const value = sessionStorage.getItem(key);
            return value ? JSON.parse(value) : null;
        } catch (error) {
            Logger.error(`Failed to retrieve from session storage: ${key}`, error);
            return null;
        }
    },
    
    remove: (key) => {
        try {
            sessionStorage.removeItem(key);
        } catch (error) {
            Logger.error(`Failed to remove from session storage: ${key}`, error);
        }
    },
    
    clear: () => {
        try {
            const refreshType = sessionStorage.getItem('refreshType');
            sessionStorage.clear();
            if (refreshType) {
                sessionStorage.setItem('refreshType', refreshType);
            }
        } catch (error) {
            Logger.error('Failed to clear session storage', error);
        }
    }
};

// Loading indicator functions
function showLoading() {
    if (loadingIndicator) {
        loadingIndicator.classList.remove('d-none');
    }
}

function hideLoading() {
    if (loadingIndicator) {
        loadingIndicator.classList.add('d-none');
    }
}

// Subject Area Filtering Functions
function updateCardVisibility() {
    const selectAllCheckbox = document.getElementById('selectAll');
    const subjectCheckboxes = document.querySelectorAll('.subject-area-checkbox:not(#selectAll)');
    const cards = document.querySelectorAll('.subject-area-card');

    // Get checked subjects
    const checkedSubjects = Array.from(
        document.querySelectorAll('.subject-area-checkbox:not(#selectAll):checked')
    ).map(cb => cb.value);
    
    // Update card visibility
    cards.forEach(card => {
        const subjectArea = card.dataset.subjectArea;
        
        const isVisible = 
            selectAllCheckbox.checked ||  // If "Select All" is checked
            checkedSubjects.length === 0 ||  // If no specific subjects are selected
            checkedSubjects.includes(subjectArea);  // If this card's subject is checked

        card.style.display = isVisible ? 'block' : 'none';
    });

    // Save checkbox state
    saveCheckboxState();
}

function saveCheckboxState() {
    const checkboxStates = {};
    const subjectCheckboxes = document.querySelectorAll('.subject-area-checkbox:not(#selectAll)');
    
    subjectCheckboxes.forEach(checkbox => {
        checkboxStates[checkbox.value] = checkbox.checked;
    });
    
    const selectAllCheckbox = document.getElementById('selectAll');
    
    SessionManager.save('checkboxStates', checkboxStates);
    SessionManager.save('selectAllState', selectAllCheckbox.checked);
}

function loadCheckboxState() {
    const savedStates = SessionManager.get('checkboxStates') || {};
    const savedSelectAllState = SessionManager.get('selectAllState');

    const selectAllCheckbox = document.getElementById('selectAll');
    const subjectCheckboxes = document.querySelectorAll('.subject-area-checkbox:not(#selectAll)');

    // Load individual subject area checkbox states
    subjectCheckboxes.forEach(checkbox => {
        checkbox.checked = savedStates[checkbox.value] || false;
    });

    // Load "Select All" state
    if (savedSelectAllState !== null) {
        selectAllCheckbox.checked = savedSelectAllState;
    }

    // Update card visibility based on loaded state
    updateCardVisibility();
}

// DAG Status Handling
function showDagStatus(subjectArea, status) {
    // Open a new window
    const newWindow = window.open('', '_blank', 'width=800,height=600,scrollbars=yes');
    
    // Sanitize the inputs before using in title
    const sanitizedSubjectArea = sanitizeHTML(subjectArea);
    const sanitizedStatus = sanitizeHTML(status);
    
    // Show loading in new window with sanitized values
    newWindow.document.write(`
        <html>
        <head>
            <title>${sanitizedSubjectArea} - ${sanitizedStatus}</title>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        </head>
        <body>
            <div id="popupContent" class="container mt-4">
                <div class="text-center" id="loadingIndicator">
                    <div class="spinner-border text-primary" role="status">
                        <span class="sr-only">Loading...</span>
                    </div>
                </div>
            </div>
        </body>
        </html>
    `);

    // Fetch data with encoded parameters
    fetch(`/dag_status?subject_area=${encodeURIComponent(subjectArea)}&status=${encodeURIComponent(status)}`, {
        method: 'GET',
        headers: {
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
    })
    .then(data => {
        const popupContent = newWindow.document.getElementById('popupContent');
        // Clear existing content
        while (popupContent.firstChild) {
            popupContent.removeChild(popupContent.firstChild);
        }

        if (!data || data.length === 0) {
            const alertDiv = newWindow.document.createElement('div');
            alertDiv.className = 'alert alert-info';
            alertDiv.textContent = `No data available for ${sanitizedSubjectArea} with status: ${sanitizedStatus}`;
            popupContent.appendChild(alertDiv);
            return;
        }

        // Create card container
        const card = newWindow.document.createElement('div');
        card.className = 'card';

        // Create card header
        const cardHeader = newWindow.document.createElement('div');
        cardHeader.className = 'card-header';
        const title = newWindow.document.createElement('h5');
        title.className = 'modal-title';
        title.textContent = `${sanitizedSubjectArea} - ${sanitizedStatus.replace(/_/g, ' ').toUpperCase()}`;
        cardHeader.appendChild(title);

        // Create card body
        const cardBody = newWindow.document.createElement('div');
        cardBody.className = 'card-body';

        // Create table responsive wrapper
        const tableResponsive = newWindow.document.createElement('div');
        tableResponsive.className = 'table-responsive';

        // Create table
        const table = newWindow.document.createElement('table');
        table.className = 'table table-striped';

        // Create table header
        const thead = newWindow.document.createElement('thead');
        const headerRow = newWindow.document.createElement('tr');
        ['DAG Name', 'Status', 'Start Time', 'End Time', 'Elapsed Time', 'Modified Time'].forEach(headerText => {
            const th = newWindow.document.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        // Create table body
        const tbody = newWindow.document.createElement('tbody');
        data.forEach(item => {
            const row = newWindow.document.createElement('tr');
            
            // DAG Name cell
            const dagNameCell = newWindow.document.createElement('td');
            const dagLink = createDagLink(item.dag_name);
            dagNameCell.appendChild(dagLink);
            
            // Status cell
            const statusCell = newWindow.document.createElement('td');
            createStatusBadge(item.status, statusCell);
            
            // Time cells - display exactly as received from database
            const startTimeCell = newWindow.document.createElement('td');
            startTimeCell.textContent = formatToEST(item.dag_start_time);
            
            const endTimeCell = newWindow.document.createElement('td');
            endTimeCell.textContent = formatToEST(item.dag_end_time);
        
            const elapsedCell = newWindow.document.createElement('td');
            elapsedCell.textContent = item.elapsed_time || '';
            
            const modifiedTimeCell = newWindow.document.createElement('td');
            modifiedTimeCell.textContent = formatToEST(item.modified_ts);
        
            // Append cells to row
            row.appendChild(dagNameCell);
            row.appendChild(statusCell);
            row.appendChild(startTimeCell);
            row.appendChild(endTimeCell);
            row.appendChild(elapsedCell);
            row.appendChild(modifiedTimeCell);
            
            tbody.appendChild(row);
        });

        // Assemble table
        table.appendChild(thead);
        table.appendChild(tbody);
        tableResponsive.appendChild(table);
        cardBody.appendChild(tableResponsive);
        card.appendChild(cardHeader);
        card.appendChild(cardBody);
        popupContent.appendChild(card);
    })
    .catch(error => {
        console.error('Error:', error);
        const popupContent = newWindow.document.getElementById('popupContent');
        
        if (popupContent) {
            // Clear any existing content first
            popupContent.innerHTML = '';
            
            // Create error message
            const errorDiv = newWindow.document.createElement('div');
            errorDiv.className = 'alert alert-danger';
            errorDiv.textContent = 'Failed to load DAG status. Please try again.';
            popupContent.appendChild(errorDiv);
            
            // If we have status info, show it but marked as potentially stale
            if (item && item.status) {
                const statusContainer = newWindow.document.createElement('div');
                statusContainer.className = 'mt-2';
                createStatusBadge(item.status, statusContainer);
                popupContent.appendChild(statusContainer);
            }
        } else {
            console.warn('Unable to show error message: Popup content element not found');
        }
    });
    
        
}

function getStatusBadgeClass(status) {
    const statusMap = {
        'success': 'success',
        'failed': 'danger',
        'running': 'primary',
        'yet_to_start': 'warning'
    };
    return statusMap[status.toLowerCase()] || 'secondary';
}





// Refresh Data Handling
async function handleRefreshClick() {
    showLoading();
    
    try {
        SessionManager.save('refreshType', 'button');
        
        // Add URL validation before fetch
        const url = new URL('/', window.location.origin);
        
        const response = await fetch(url.toString(), {
            method: 'GET',
            headers: {
                'Accept': 'text/html',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'same-origin'  // Ensure same-origin policy
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        window.location.reload();
    } catch (error) {
        Logger.error('Error refreshing data', error);
        alert('Failed to refresh data. Please try again.');
        hideLoading();
    }
}

// Initialization and Event Listeners
document.addEventListener('DOMContentLoaded', function() {
    // Setup loading indicator
    loadingIndicator = document.getElementById('loadingIndicator');

    // Setup subject area filtering
    const selectAllCheckbox = document.getElementById('selectAll');
    const subjectCheckboxes = document.querySelectorAll('.subject-area-checkbox:not(#selectAll)');

    // Select All Checkbox Logic
    selectAllCheckbox.addEventListener('change', function() {
        const isChecked = this.checked;

        // Check/Uncheck all subject area checkboxes
        subjectCheckboxes.forEach(checkbox => {
            checkbox.checked = isChecked;
        });

        // Update card visibility
        updateCardVisibility();
    });

    // Individual Subject Checkbox Logic
    subjectCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            // Update "Select All" state if all checkboxes are checked/unchecked
            const allChecked = Array.from(subjectCheckboxes)
                .every(cb => cb.checked);
            
            selectAllCheckbox.checked = allChecked;
            updateCardVisibility();
        });
    });

    // Setup refresh button
    const refreshButton = document.getElementById('refreshDataButton');
    if (refreshButton) {
        refreshButton.addEventListener('click', handleRefreshClick);
    }

    // Load initial checkbox states
    loadCheckboxState();

    // Global function for showing DAG status
    window.showDagStatus = showDagStatus;

    // Error handling
    window.addEventListener('unhandledrejection', function(event) {
        Logger.error('Unhandled promise rejection', event.reason);
        hideLoading();
    });
});
// Package Parser Functionality
function initializePackageParser() {
    const form = document.getElementById('packageForm');
    const customSearch = document.getElementById('custom-search');
    const radioButtons = document.querySelectorAll('.package-radio');
    const packageInput = document.getElementById('package_name');

    // Initialize radio button handlers
    radioButtons.forEach(radio => {
        radio.addEventListener('change', function() {
            handleRadioChange(this, customSearch);
        });
    });

    // Form submission handler
    if (form) {
        form.addEventListener('submit', function(e) {
            if (!validateForm(this)) {
                e.preventDefault();
            }
        });
    }

    // Set initial state based on URL parameters
    setInitialState();
}

function handleRadioChange(radio, customSearch) {
    if (radio.id === 'custom') {
        showElement(customSearch);
    } else {
        hideElement(customSearch);
    }
}

function validateForm(form) {
    const customRadio = document.getElementById('custom');
    const packageInput = document.getElementById('package_name');

    if (customRadio.checked && (!packageInput.value || packageInput.value.trim() === '')) {
        showError('Please enter at least one package name');
        return false;
    }
    return true;
}

function showError(message) {
    // Remove any existing error messages
    const existingError = document.querySelector('.error-message');
    if (existingError) {
        existingError.remove();
    }

    // Create and show new error message
    const errorDiv = document.createElement('div');
    errorDiv.className = 'alert alert-danger error-message mt-2';
    errorDiv.textContent = message;
    
    const form = document.getElementById('packageForm');
    form.insertAdjacentElement('afterend', errorDiv);

    // Remove error message after 3 seconds
    setTimeout(() => {
        errorDiv.remove();
    }, 3000);
}

function setInitialState() {
    const urlParams = new URLSearchParams(window.location.search);
    const searchType = urlParams.get('search_type');
    const customSearch = document.getElementById('custom-search');

    if (searchType === 'custom') {
        document.getElementById('custom').checked = true;
        showElement(customSearch);
    } else {
        document.getElementById('today').checked = true;
        hideElement(customSearch);
    }
}

// Add this to your existing document.ready function or create a new one if it doesn't exist
document.addEventListener('DOMContentLoaded', function() {
    // Your existing initialization code
    
    // Initialize package parser if we're on the package parser page
    if (document.getElementById('packageForm')) {
        initializePackageParser();
    }
});
function showElement(element) {
    if (element) {
        element.style.display = 'block';
    }
}

function hideElement(element) {
    if (element) {
        element.style.display = 'none';
    }
}
// Package Parser Functions
function initializePackageParser() {
    const searchInput = document.getElementById('packageSearch');
    if (searchInput) {
        searchInput.addEventListener('input', handlePackageSearch);
    }

    // Start notification refresh cycle
    refreshNotifications();
    // Refresh notifications every 5 minutes
    setInterval(refreshNotifications, 300000);
}

function handlePackageSearch(event) {
    const searchTerm = event.target.value.toLowerCase().trim();
    const packages = document.querySelectorAll('.package-card');
    
    packages.forEach(package => {
        const packageName = package.dataset.packageName.toLowerCase();
        const isVisible = packageName.includes(searchTerm);
        package.style.display = isVisible ? 'block' : 'none';
        
        // Add animation for smooth transitions
        if (isVisible) {
            package.classList.add('fade-in');
            package.classList.remove('fade-out');
        } else {
            package.classList.add('fade-out');
            package.classList.remove('fade-in');
        }
    });
}

function refreshNotifications() {
    showLoading();
    
    fetch('/api/notifications', {
        method: 'GET',
        headers: {
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
    })
    .then(data => {
        updateNotificationPanel(data.notifications);
    })
    .catch(error => {
        Logger.error('Failed to refresh notifications:', error);
        showNotificationError();
    })
    .finally(() => {
        hideLoading();
    });
}

function updateNotificationPanel(notifications) {
    const notificationList = document.getElementById('notificationList');
    if (!notificationList) return;

    try {
        const notificationHTML = notifications.map(notification => {
            const sanitizedPackageName = sanitizeHTML(notification.package_name);
            const sanitizedVersion = sanitizeHTML(notification.new_version);
            const formattedTime = formatToEST(notification.update_time);
            
            return `
                <div class="alert alert-info fade-in">
                    <strong>${sanitizedPackageName}</strong> was updated to 
                    version ${sanitizedVersion}
                    <small class="d-block text-muted">
                        ${formattedTime}
                    </small>
                </div>
            `;
        }).join('');

        notificationList.innerHTML = notificationHTML;
    } catch (error) {
        Logger.error('Error updating notification panel:', error);
        showNotificationError();
    }
}

function showNotificationError() {
    const notificationList = document.getElementById('notificationList');
    if (notificationList) {
        notificationList.innerHTML = `
            <div class="alert alert-danger">
                Failed to load notifications. Please try again later.
            </div>
        `;
    }
}

// Initialize package parser when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializePackageParser();
});

// Package Parser Functions
function handlePackageSearch(event) {
    const searchTerm = event.target.value.toLowerCase().trim();
    const packages = document.querySelectorAll('.package-card');
    let hasResults = false;
    
    packages.forEach(package => {
        const packageName = package.dataset.packageName.toLowerCase();
        const isVisible = packageName.includes(searchTerm);
        package.style.display = isVisible ? 'block' : 'none';
        if (isVisible) hasResults = true;
    });
    
    document.getElementById('noPackagesFound').style.display = 
        hasResults ? 'none' : 'block';
}

// Add this to your existing document.ready function
document.addEventListener('DOMContentLoaded', function() {
    // Your existing initialization code
    
    // Initialize package search if we're on the package parser page
    const searchInput = document.getElementById('packageSearch');
    if (searchInput) {
        searchInput.addEventListener('input', handlePackageSearch);
    }
});




