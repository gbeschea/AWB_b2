document.addEventListener('DOMContentLoaded', () => {
    const tableBody = document.querySelector('.main-table tbody');

    if (tableBody) {
        tableBody.addEventListener('click', async (event) => {
            const validButton = event.target.closest('.mark-valid-btn');
            const holdButton = event.target.closest('.hold-btn');

            if (validButton) {
                const orderRow = validButton.closest('tr');
                const orderId = orderRow.dataset.orderId;
                
                try {
                    const response = await fetch(`/validation/${orderId}/mark-valid`, { method: 'POST' });
                    if (response.ok) {
                        orderRow.style.opacity = '0';
                        setTimeout(() => orderRow.remove(), 300);
                    } else {
                        alert('A apărut o eroare la marcarea ca validă.');
                    }
                } catch (error) {
                    console.error('Eroare:', error);
                    alert('Eroare de rețea.');
                }
            }

            if (holdButton) {
                const orderRow = holdButton.closest('tr');
                const orderId = orderRow.dataset.orderId;
                
                try {
                    const response = await fetch(`/validation/${orderId}/hold`, { method: 'POST' });
                    if (response.ok) {
                        orderRow.style.opacity = '0';
                        setTimeout(() => orderRow.remove(), 300);
                    } else {
                        alert('A apărut o eroare la punerea pe hold.');
                    }
                } catch (error) {
                    console.error('Eroare:', error);
                    alert('Eroare de rețea.');
                }
            }
        });
    }
});

// Funcție pentru a salva modificările
const saveAddressChange = async (inputElement) => {
    const orderRow = inputElement.closest('tr');
    const orderId = orderRow.dataset.orderId;
    const field = inputElement.dataset.field;
    const value = inputElement.value;
    const originalValue = inputElement.dataset.originalValue;

    // Nu facem nimic dacă valoarea nu s-a schimbat
    if (value === originalValue) {
        const newSpan = document.createElement('span');
        newSpan.className = 'editable';
        newSpan.dataset.field = field;
        newSpan.textContent = value;
        inputElement.replaceWith(newSpan);
        return;
    }

    // Trimitem datele la server
    try {
        const response = await fetch(`/validation/${orderId}/address`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ field, value })
        });

        if (!response.ok) throw new Error('Salvare eșuată');
        
        const result = await response.json();
        
        // Înlocuim input-ul cu noul span
        const newSpan = document.createElement('span');
        newSpan.className = 'editable';
        newSpan.dataset.field = field;
        newSpan.textContent = value;
        inputElement.replaceWith(newSpan);

        // Actualizăm vizual statusul validării pe pagină
        // (Vom adăuga elemente HTML pentru a afișa noile erori/scor mai târziu)
        console.log('Re-validare:', result);
        alert(`Adresa actualizată! Noul status: ${result.new_status}, Scor: ${result.new_score}`);


    } catch (error) {
        console.error('Eroare la salvare:', error);
        alert('Nu s-a putut salva modificarea.');
        // Revenim la valoarea originală în caz de eroare
        const newSpan = document.createElement('span');
        newSpan.className = 'editable';
        newSpan.dataset.field = field;
        newSpan.textContent = originalValue;
        inputElement.replaceWith(newSpan);
    }
};

// Logica pentru a transforma span-ul în input la click
if (tableBody) {
    tableBody.addEventListener('click', (event) => {
        const target = event.target;
        // Verificăm dacă am dat click pe un element editabil ȘI nu există deja un input activ
        if (target.classList.contains('editable') && !target.querySelector('input')) {
            const originalValue = target.textContent;
            const field = target.dataset.field;

            const input = document.createElement('input');
            input.type = 'text';
            input.value = originalValue;
            input.dataset.field = field;
            input.dataset.originalValue = originalValue;
            
            // Adăugăm event listener pentru a salva la Enter sau la click în afară
            input.addEventListener('blur', () => saveAddressChange(input));
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    input.blur();
                } else if (e.key === 'Escape') {
                    // Anulăm editarea la Escape
                    const newSpan = document.createElement('span');
                    newSpan.className = 'editable';
                    newSpan.dataset.field = field;
                    newSpan.textContent = originalValue;
                    input.replaceWith(newSpan);
                }
            });

            target.textContent = '';
            target.appendChild(input);
            input.focus();
        }
    });
}
