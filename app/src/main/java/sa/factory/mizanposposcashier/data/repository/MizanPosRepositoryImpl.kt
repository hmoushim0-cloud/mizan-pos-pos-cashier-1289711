package sa.factory.mizanposposcashier.data.repository

import sa.factory.mizanposposcashier.domain.repository.MizanPosRepository
import sa.factory.mizanposposcashier.hardware.HardwareSensorDriver
import sa.factory.mizanposposcashier.model.MizanPosItem
import sa.factory.mizanposposcashier.model.MizanPosOrder
import sa.factory.mizanposposcashier.model.MizanPosTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class MizanPosRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : MizanPosRepository {

    private val _catalog = MutableStateFlow<List<MizanPosItem>>(listOf(
        MizanPosItem("1", "حزمة تدريب موظفين", "تدريب عملي على نظام سقار مع شهادة معتمدة", 450.0, "معتمد", "مبيعات فورية"),
        MizanPosItem("2", "حبر طابعة حراري", "شريط حبر أصلي لطابعات ESC/POS - 50 متر", 85.0, "أصلي", "إدارة مخزون"),
        MizanPosItem("3", "مسدس الباركود اللاسلكي", "مدى 20 متر مع قاعدة شحن مغناطيسية", 680.0, "جديد", "تقارير يومية")
    ))
    override val catalogFlow: Flow<List<MizanPosItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<MizanPosItem>>(emptyList())
    override val activeCart: StateFlow<List<MizanPosItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<MizanPosTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            MizanPosTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: MizanPosItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: MizanPosItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): MizanPosOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = MizanPosOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
