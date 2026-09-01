(function () {
    const html = document.documentElement;
    const shell = document.getElementById('appShell');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarClose = document.getElementById('sidebarClose');
    const sidebarBackdrop = document.getElementById('sidebarBackdrop');
    const themeToggle = document.getElementById('themeToggle');
    const desktopQuery = window.matchMedia('(min-width: 992px)');

    function setTheme(theme) {
        html.setAttribute('data-bs-theme', theme);
        localStorage.setItem('openledger-theme', theme);
        if (themeToggle) {
            themeToggle.setAttribute(
                'aria-label',
                theme === 'dark' ? 'Use light theme' : 'Use dark theme'
            );
        }
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

    const storedTheme = localStorage.getItem('openledger-theme');
    setTheme(storedTheme || 'dark');

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
    if (themeToggle) {
        themeToggle.addEventListener('click', function () {
            setTheme(html.getAttribute('data-bs-theme') === 'dark' ? 'light' : 'dark');
        });
    }
    if (shell) desktopQuery.addEventListener('change', closeMobileSidebar);

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') closeMobileSidebar();
    });

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
