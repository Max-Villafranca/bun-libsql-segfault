import { createClient, Client as LibSqlClient, Transaction as LibSqlTransaction } from '@libsql/client'

// --- Configuration ---
const DB_URL = 'file:./local_minimal_test.db'
const NUM_ITEMS_TO_SEED = 2
const ITEMS_TO_UPDATE_IN_LOOP = ['item_m_0', 'item_m_1']
const TRANSACTIONS_IN_BURST = 10 // Changing it will affect the test
const NUM_BURSTS = 4 // Changing it will affect the test
const DELAY_SHORT_MS = 200 // Between transactions in a burst
const DELAY_LONG_MS = 1000 // Between bursts

async function setupDatabase(client: LibSqlClient) {
    console.log('[SETUP] Ensuring table and seeding items...')
    try {
        await client.execute(`
            CREATE TABLE IF NOT EXISTS items_minimal (
                id TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
        `)
        // Clear items for a somewhat consistent state for value updates
        await client.execute(`DELETE FROM items_minimal;`)
    } catch (e) {
        console.warn('[SETUP] Could not create/clear table (might be okay if it exists):', e)
    }

    for (let i = 0; i < NUM_ITEMS_TO_SEED; i++) {
        const id = `item_m_${i}`
        try {
            await client.execute({
                sql: 'INSERT INTO items_minimal (id, value) VALUES (?, 0);',
                args: [id],
            })
        } catch (error) {
            console.warn(`[SETUP] Could not insert ${id}:`, error)
        }
    }
    console.log(`[SETUP] Seeded/Ensured ${NUM_ITEMS_TO_SEED} items.`)
}

async function performTransactionWithLoop(client: LibSqlClient, label: string) {
    console.log(`\n[${label}] Attempting transaction with internal update loop...`)
    let tx: LibSqlTransaction | undefined = undefined
    try {
        tx = await client.transaction('write')
        // console.log(`  [${label}] Transaction started.`);

        for (const itemId of ITEMS_TO_UPDATE_IN_LOOP) {
            const result = await tx.execute({ sql: 'SELECT value FROM items_minimal WHERE id = ?;', args: [itemId] })
            const currentValue = (result.rows[0]?.value as number) ?? -1 // Default to -1 if not found
            if (currentValue === -1) {
                // console.warn(`    [${label}] Item ${itemId} not found in DB for update.`);
                // Continue to next item or throw, for repro let's try to continue
                continue
            }
            const newValue = currentValue + 1

            // console.log(`    [${label}] Updating ${itemId} from ${currentValue} to ${newValue}`);
            await tx.execute({ sql: 'UPDATE items_minimal SET value = ? WHERE id = ?;', args: [newValue, itemId] })
            // console.log(`    [${label}] ${itemId} update awaited.`);
        }
        // console.log(`  [${label}] Committing...`);
        await tx.commit()
        console.log(`[${label}] Transaction successful!`)
        return true
    } catch (error) {
        console.error(`[${label}] Transaction FAILED:`, error)
        if (tx) {
            try {
                await tx.rollback()
            } catch (rbError) {
                /* ignore rollback error */
            }
        }
        return false
    }
}

async function main() {
    console.log('--- Minimal Direct LibSQL Client Test ---')
    console.log('--- This script will use the globally installed @libsql/client version ---')

    const client = createClient({ url: DB_URL })
    console.log('LibSQL client initialized.')

    try {
        await setupDatabase(client)

        for (let burst = 0; burst < NUM_BURSTS; burst++) {
            console.log(`\n===== STARTING BURST ${burst + 1} of ${NUM_BURSTS} =====`)
            for (let i = 0; i < TRANSACTIONS_IN_BURST; i++) {
                const label = `B${burst + 1}/T${i + 1}`
                await performTransactionWithLoop(client, label)

                if (i < TRANSACTIONS_IN_BURST - 1) {
                    await new Promise(resolve => setTimeout(resolve, DELAY_SHORT_MS))
                }
            }
            if (burst < NUM_BURSTS - 1) {
                console.log(`===== ENDING BURST ${burst + 1}, Pausing for ${DELAY_LONG_MS}ms =====`)
                await new Promise(resolve => setTimeout(resolve, DELAY_LONG_MS))
            }
        }
    } catch (e) {
        console.error('FATAL: Unhandled error in main test loop:', e)
    } finally {
        console.log('\n--- Minimal Test finished (or crashed) ---')
        console.log('Closing LibSQL client...')
        try {
            client.close()
            console.log('Client closed.')
        } catch (closeError) {
            console.error('Error closing client:', closeError)
        }
    }
}

main().catch(e => {
    console.error('FATAL: Unhandled error from top-level main promise:', e)
    process.exit(1)
})
