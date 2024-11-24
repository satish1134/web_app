// Global variables
let loadingIndicator;
let DEBUG = false; // Disable detailed logging in production

// Simplified logging utility
const Logger = {
    error: (message, error = null) => {
        console.error(`[ERROR] ${message}`, error || '');
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

// Checkbox state management
function saveCheckboxState() {
    const checkboxStates = {};
    document.querySelectorAll('.subject-area-checkbox').forEach(checkbox => {
        checkboxStates[checkbox.value] = checkbox.checked;
    });
    
    SessionManager.save('checkboxStates', checkboxStates);
    SessionManager.save('selectAllState', document.getElementById('selectAll').checked);
    SessionManager.save('lastSaved', new Date().toISOString());
}

function loadCheckboxState() {
    const checkboxStates = SessionManager.get('checkboxStates');
    const selectAllState = SessionManager.get('selectAllState');
    
    if (checkboxStates) {
        document.querySelectorAll('.subject-area-checkbox').forEach(checkbox => {
            checkbox.checked = checkboxStates[checkbox.value] || false;
        });
        
        const selectAllCheckbox = document.getElementById('selectAll');
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = selectAllState || false;
        }
        
        updateCardVisibility();
    }
}

function clearFilters() {
    SessionManager.clear();
    
    document.querySelectorAll('.subject-area-checkbox').forEach(checkbox => {
        checkbox.checked = false;
    });
    
    const selectAllCheckbox = document.getElementById('selectAll');
    if (selectAllCheckbox) {
        selectAllCheckbox.checked = false;
    }
    
    document.querySelectorAll('.subject-area-card').forEach(card => {
        card.style.display = 'block';
    });
}

function updateCardVisibility() {
    const checkedSubjects = Array.from(document.querySelectorAll('.subject-area-checkbox:checked'))
        .map(cb => cb.value);
    
    const cards = document.querySelectorAll('.subject-area-card');
    
    cards.forEach(card => {
        const shouldShow = checkedSubjects.length === 0 || 
                          checkedSubjects.includes(card.dataset.subjectArea);
        card.style.display = shouldShow ? 'block' : 'none';
    });
    
    saveCheckboxState();
}

// DAG status handling
function showDagStatus(subjectArea, status) {
    const popup = document.getElementById('popup');
    const popupContent = document.getElementById('popupContent');
    const modalTitle = document.getElementById('modalTitle');
    
    if (!popup || !popupContent) {
        Logger.error('Required popup elements not found');
        return;
    }

    if (modalTitle) {
        modalTitle.textContent = `${subjectArea} - ${status.replace(/_/g, ' ').toUpperCase()}`;
    }

    showLoading();
    popup.style.display = 'block';

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
        hideLoading();
        
        if (!data || data.length === 0) {
            popupContent.innerHTML = `
                <div class="alert alert-info">
                    No data available for ${subjectArea} with status: ${status}
                </div>`;
            return;
        }

        let tableHTML = `
            <div class="table-responsive">
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>DAG Name</th>
                            <th>Status</th>
                            <th>Start Time</th>
                            <th>End Time</th>
                            <th>Modified Time</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        data.forEach(item => {
            const sanitize = (value) => {
                if (window.DOMPurify) {
                    return DOMPurify.sanitize(value || '');
                }
                return value || '';
            };

            tableHTML += `
                <tr>
                    <td>${sanitize(item.dag_name)}</td>
                    <td>
                        <span class="badge badge-${getStatusBadgeClass(item.status)}">
                            ${sanitize(item.status)}
                        </span>
                    </td>
                    <td>${sanitize(item.dag_start_time)}</td>
                    <td>${sanitize(item.dag_end_time)}</td>
                    <td>${sanitize(item.modified_ts)}</td>
                </tr>
            `;
        });

        tableHTML += '</tbody></table></div>';
        popupContent.innerHTML = tableHTML;
    })
    .catch(error => {
        hideLoading();
        Logger.error('Error in showDagStatus', error);
        popupContent.innerHTML = `
            <div class="alert alert-danger">
                <h5>Error loading data</h5>
                <p>${error.message}</p>
                <p>Please try again or contact support if the problem persists.</p>
            </div>
        `;
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

// Initialize application
document.addEventListener('DOMContentLoaded', function() {
    loadingIndicator = document.getElementById('loadingIndicator');
    window.showDagStatus = showDagStatus;

    // Setup close popup functionality
    const closePopupBtn = document.getElementById('closePopup');
    const popup = document.getElementById('popup');

    if (closePopupBtn && popup) {
        closePopupBtn.addEventListener('click', () => {
            popup.style.display = 'none';
        });

        popup.addEventListener('click', (e) => {
            if (e.target.id === 'popup') {
                popup.style.display = 'none';
            }
        });
    }

    // Setup subject area dropdown
    const subjectAreaDropdown = document.getElementById('subjectAreaDropdown');
    if (subjectAreaDropdown) {
        initializeSubjectAreaDropdown(subjectAreaDropdown);
    }

    // Setup refresh button
    const refreshButton = document.getElementById('refreshDataButton');
    if (refreshButton) {
        refreshButton.addEventListener('click', handleRefreshClick);
    }

    // Add keyboard support for closing popup
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && popup && popup.style.display === 'block') {
            popup.style.display = 'none';
        }
    });
});

function initializeSubjectAreaDropdown(dropdown) {
    const dropdownMenu = createDropdownMenu();
    dropdown.appendChild(dropdownMenu);

    const selectAllCheckbox = document.getElementById('selectAll');
    const subjectCheckboxes = document.querySelectorAll('.subject-area-checkbox');

    setupCheckboxEventListeners(selectAllCheckbox, subjectCheckboxes);
    handlePageRefresh();
}

function createDropdownMenu() {
    const dropdownMenu = document.createElement('div');
    dropdownMenu.className = 'dropdown-menu';
    dropdownMenu.setAttribute('aria-labelledby', 'subjectAreaDropdown');

    // Add Select All option
    const selectAllItem = createSelectAllItem();
    dropdownMenu.appendChild(selectAllItem);
    dropdownMenu.appendChild(document.createElement('div')).className = 'dropdown-divider';

    // Add subject area options
    addSubjectAreaOptions(dropdownMenu);

    return dropdownMenu;
}

function createSelectAllItem() {
    const selectAllItem = document.createElement('div');
    selectAllItem.className = 'dropdown-item';
    selectAllItem.innerHTML = `
        <div class="form-check">
            <input class="form-check-input" type="checkbox" id="selectAll">
            <label class="form-check-label" for="selectAll">Select All</label>
        </div>
    `;
    return selectAllItem;
}

function addSubjectAreaOptions(dropdownMenu) {
    const cards = document.querySelectorAll('.subject-area-card');
    const subjectAreas = new Set();
    cards.forEach(card => subjectAreas.add(card.dataset.subjectArea));

    subjectAreas.forEach(subjectArea => {
        const item = document.createElement('div');
        item.className = 'dropdown-item';
        item.innerHTML = `
            <div class="form-check">
                <input class="form-check-input subject-area-checkbox" type="checkbox" 
                       id="check_${subjectArea}" value="${subjectArea}">
                <label class="form-check-label" for="check_${subjectArea}">
                    ${subjectArea}
                </label>
            </div>
        `;
        dropdownMenu.appendChild(item);
    });
}

function setupCheckboxEventListeners(selectAllCheckbox, subjectCheckboxes) {
    selectAllCheckbox.addEventListener('change', function() {
        const isChecked = this.checked;
        subjectCheckboxes.forEach(checkbox => {
            checkbox.checked = isChecked;
        });
        updateCardVisibility();
    });

    subjectCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            selectAllCheckbox.checked = Array.from(subjectCheckboxes)
                .every(cb => cb.checked);
            updateCardVisibility();
        });
    });
}

function handlePageRefresh() {
    const refreshType = SessionManager.get('refreshType');
    const isNormalRefresh = performance.navigation.type === performance.navigation.TYPE_RELOAD;
    
    if (isNormalRefresh && refreshType !== 'button') {
        clearFilters();
    } else {
        loadCheckboxState();
    }
    
    SessionManager.remove('refreshType');
}

async function handleRefreshClick() {
    showLoading();
    
    try {
        SessionManager.save('refreshType', 'button');
        const response = await fetch(window.location.href);
        
        if (!response.ok) throw new Error('Refresh failed');
        window.location.reload();
    } catch (error) {
        Logger.error('Error refreshing data', error);
        alert('Failed to refresh data. Please try again.');
        hideLoading();
    }
}

// Error handling
window.addEventListener('unhandledrejection', function(event) {
    Logger.error('Unhandled promise rejection', event.reason);
    hideLoading();
});