(() => {
    'use strict';

    const form = document.getElementById('combineCaseForm');
    const search = document.getElementById('combineCaseSearch');
    const list = document.getElementById('combineCaseList');
    const count = document.getElementById('combinedCaseSelectionCount');
    const submit = document.getElementById('combineCaseSubmit');
    const validation = document.getElementById('combineCaseValidation');
    const noResults = document.getElementById('combineCaseNoResults');
    if (!form || !search || !list || !count || !submit || !validation) return;

    const maximum = Number(form.dataset.maxCases || 10);
    const options = Array.from(list.querySelectorAll('.combine-case-option'));
    const checkboxes = options.map((option) => option.querySelector('input[type="checkbox"]'));

    const updateSelection = () => {
        const selected = checkboxes.filter((checkbox) => checkbox.checked);
        count.textContent = `${selected.length} selected`;
        checkboxes.forEach((checkbox) => {
            checkbox.disabled = !checkbox.checked && selected.length >= maximum;
        });
        submit.disabled = selected.length < 2 || selected.length > maximum;
        if (selected.length < 2) {
            validation.textContent = 'Select at least two cases to continue.';
        } else if (selected.length >= maximum) {
            validation.textContent = `${maximum} cases selected — the initial safety limit has been reached.`;
        } else {
            validation.textContent = `${selected.length} source cases will remain independently auditable.`;
        }
    };

    const filterCases = () => {
        const query = search.value.trim().toLocaleLowerCase();
        let visible = 0;
        options.forEach((option) => {
            const matches = !query || String(option.dataset.caseSearch || '').includes(query);
            option.hidden = !matches;
            if (matches) visible += 1;
        });
        if (noResults) noResults.hidden = visible !== 0;
    };

    checkboxes.forEach((checkbox) => checkbox.addEventListener('change', updateSelection));
    search.addEventListener('input', filterCases);
    updateSelection();
})();
