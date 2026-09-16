(function () {
    const html = document.documentElement;
    const shell = document.getElementById('appShell');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarClose = document.getElementById('sidebarClose');
    const sidebarBackdrop = document.getElementById('sidebarBackdrop');
    const workspaceClock = document.getElementById('workspaceClock');
    const desktopQuery = window.matchMedia('(min-width: 992px)');

    function setOperationalTheme() {
        html.setAttribute('data-bs-theme', 'dark');
        localStorage.removeItem('openledger-theme');
    }

    function updateWorkspaceClock() {
        if (!workspaceClock) return;
        const parts = new Intl.DateTimeFormat('en-GB', {
            timeZone: 'Asia/Jakarta',
            day: '2-digit',
            month: 'short',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        }).formatToParts(new Date());
        const values = Object.fromEntries(
            parts.filter(part => part.type !== 'literal')
                .map(part => [part.type, part.value])
        );
        workspaceClock.textContent = `${values.day} ${values.month.toUpperCase()} ${values.year} · ${values.hour}:${values.minute} WIB`;
    }

    function closeMobileSidebar() {
        if (!shell || !sidebarToggle) return;
        shell.classList.remove('sidebar-open');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    }

    function toggleSidebar() {
        if (!shell || !sidebarToggle) return;
        if (desktopQuery.matches) {
            const collapsed = shell.classList.toggle('sidebar-collapsed');
            localStorage.setItem('openledger-sidebar-collapsed', String(collapsed));
            sidebarToggle.setAttribute('aria-expanded', String(!collapsed));
        } else {
            const opened = shell.classList.toggle('sidebar-open');
            sidebarToggle.setAttribute('aria-expanded', String(opened));
        }
    }

    setOperationalTheme();
    updateWorkspaceClock();
    if (workspaceClock) window.setInterval(updateWorkspaceClock, 30000);

    if (
        shell &&
        sidebarToggle &&
        desktopQuery.matches &&
        localStorage.getItem('openledger-sidebar-collapsed') === 'true'
    ) {
        shell.classList.add('sidebar-collapsed');
        sidebarToggle.setAttribute('aria-expanded', 'false');
    } else if (sidebarToggle && desktopQuery.matches) {
        sidebarToggle.setAttribute('aria-expanded', 'true');
    }

    if (sidebarToggle) sidebarToggle.addEventListener('click', toggleSidebar);
    if (sidebarClose) sidebarClose.addEventListener('click', closeMobileSidebar);
    if (sidebarBackdrop) {
        sidebarBackdrop.addEventListener('click', closeMobileSidebar);
    }
    if (shell) desktopQuery.addEventListener('change', closeMobileSidebar);

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') closeMobileSidebar();
    });

    function initializeSortableTable(table) {
        const buttons = Array.from(table.querySelectorAll('[data-sort-column]'));
        if (!buttons.length || !table.tBodies.length) return;
        if (table.dataset.serverSort === 'true') {
            const parameters = new URLSearchParams(window.location.search);
            const activeKey = parameters.get('sort') || 'default';
            const activeDirection = parameters.get('direction') || 'ascending';
            buttons.forEach(function (button) {
                button.addEventListener('click', function () {
                    const key = button.dataset.sortKey;
                    parameters.set('sort', key);
                    parameters.set(
                        'direction',
                        activeKey === key && activeDirection === 'ascending'
                            ? 'descending'
                            : 'ascending'
                    );
                    parameters.delete('page');
                    window.location.assign(
                        window.location.pathname + '?' + parameters.toString()
                        + '#operator-review'
                    );
                });
            });
            return;
        }
        let activeColumn = null;
        let direction = 'ascending';

        function valueFor(row, column) {
            const cell = row.cells[column];
            return String(cell?.dataset.sortValue || cell?.textContent || '').trim();
        }

        function applySort() {
            if (activeColumn === null) return;
            const body = table.tBodies[0];
            const rows = Array.from(body.rows).filter(row => row.cells.length > activeColumn);
            rows.sort(function (left, right) {
                const comparison = valueFor(left, activeColumn).localeCompare(
                    valueFor(right, activeColumn),
                    undefined,
                    { numeric: true, sensitivity: 'base' }
                );
                return direction === 'ascending' ? comparison : -comparison;
            });
            rows.forEach(row => body.appendChild(row));
        }

        buttons.forEach(function (button) {
            button.addEventListener('click', function () {
                const column = Number(button.dataset.sortColumn);
                direction = activeColumn === column && direction === 'ascending'
                    ? 'descending'
                    : 'ascending';
                activeColumn = column;
                buttons.forEach(function (candidate) {
                    const selected = Number(candidate.dataset.sortColumn) === activeColumn;
                    candidate.closest('th')?.setAttribute(
                        'aria-sort', selected ? direction : 'none'
                    );
                });
                applySort();
            });
        });
        table.addEventListener('openledger:table-updated', applySort);
    }

    document.querySelectorAll('[data-sortable-table]').forEach(initializeSortableTable);

    const caseDeleteForms = document.querySelectorAll('.case-delete-form');
    const caseDeleteElement = document.getElementById('caseDeleteModal');
    const caseDeleteExpected = document.getElementById('caseDeleteExpected');
    const caseDeleteInput = document.getElementById('caseDeleteConfirmation');
    const caseDeleteConfirm = document.getElementById('caseDeleteConfirm');
    const caseDeleteCopy = document.getElementById('caseDeleteCopy');
    const caseDeleteCopyStatus = document.getElementById('caseDeleteCopyStatus');
    const caseDeleteError = document.getElementById('caseDeleteError');

    if (
        caseDeleteForms.length
        && caseDeleteElement
        && caseDeleteExpected
        && caseDeleteInput
        && caseDeleteConfirm
        && caseDeleteCopy
        && caseDeleteCopyStatus
        && caseDeleteError
        && window.bootstrap
    ) {
        const caseDeleteModal = new window.bootstrap.Modal(caseDeleteElement);
        let pendingDeleteForm = null;
        let expectedCaseTitle = '';

        function setDeleteValidation(showError) {
            const matches = Boolean(expectedCaseTitle) && caseDeleteInput.value === expectedCaseTitle;
            caseDeleteConfirm.disabled = !matches;
            caseDeleteInput.setAttribute('aria-invalid', String(showError && !matches));
            caseDeleteError.hidden = !showError || matches;
            return matches;
        }

        function resetDeleteModal() {
            pendingDeleteForm = null;
            expectedCaseTitle = '';
            caseDeleteExpected.textContent = '';
            caseDeleteInput.value = '';
            caseDeleteInput.removeAttribute('aria-invalid');
            caseDeleteConfirm.disabled = true;
            caseDeleteError.hidden = true;
            caseDeleteCopyStatus.textContent = '';
        }

        function fallbackCopy(textToCopy) {
            const helper = document.createElement('textarea');
            helper.value = textToCopy;
            helper.setAttribute('readonly', '');
            helper.style.position = 'fixed';
            helper.style.opacity = '0';
            document.body.appendChild(helper);
            helper.select();
            const copied = document.execCommand('copy');
            helper.remove();
            if (!copied) throw new Error('Copy command was rejected.');
        }

        async function copyCaseTitle() {
            try {
                if (navigator.clipboard && window.isSecureContext) {
                    await navigator.clipboard.writeText(expectedCaseTitle);
                } else {
                    fallbackCopy(expectedCaseTitle);
                }
                caseDeleteCopyStatus.textContent = 'Case name copied.';
            } catch (_error) {
                caseDeleteCopyStatus.textContent = 'Copy failed. Select the case name and copy it manually.';
            }
        }

        caseDeleteForms.forEach(function (form) {
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                pendingDeleteForm = form;
                expectedCaseTitle = form.dataset.caseTitle || '';
                caseDeleteExpected.textContent = expectedCaseTitle;
                caseDeleteInput.value = '';
                caseDeleteCopyStatus.textContent = '';
                setDeleteValidation(false);
                caseDeleteModal.show();
            });
        });

        caseDeleteInput.addEventListener('input', function () {
            setDeleteValidation(false);
        });
        caseDeleteInput.addEventListener('keydown', function (event) {
            if (event.key !== 'Enter') return;
            event.preventDefault();
            if (setDeleteValidation(true)) caseDeleteConfirm.click();
        });
        caseDeleteCopy.addEventListener('click', copyCaseTitle);
        caseDeleteConfirm.addEventListener('click', function () {
            if (!pendingDeleteForm || !setDeleteValidation(true)) return;
            const confirmation = pendingDeleteForm.querySelector('input[name="confirmation_name"]');
            if (!confirmation) return;
            confirmation.value = caseDeleteInput.value;
            caseDeleteConfirm.disabled = true;
            HTMLFormElement.prototype.submit.call(pendingDeleteForm);
        });
        caseDeleteElement.addEventListener('shown.bs.modal', function () {
            caseDeleteInput.focus();
        });
        caseDeleteElement.addEventListener('hidden.bs.modal', resetDeleteModal);
    }

    if (window.lucide) window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } });
})();
