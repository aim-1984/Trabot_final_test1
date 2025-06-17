from PyQt5.QtWidgets import QWidget, QVBoxLayout, QSlider, QRadioButton, QPushButton, QLabel, QHBoxLayout, QMessageBox
from PyQt5.QtCore import Qt
from config.settings import Settings

class RiskProfileWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Настройка риск-профиля")
        self.setFixedSize(400, 250)

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        # Ползунок
        self.slider_label = QLabel("Уровень риска: 0")
        self.slider = QSlider(Qt.Horizontal)
        self.slider.setMinimum(0)
        self.slider.setMaximum(100)
        self.slider.setValue(0)
        self.slider.valueChanged.connect(self.update_slider_label)

        # Переключатели
        self.low_risk = QRadioButton("Низкий риск")
        self.mid_risk = QRadioButton("Средний риск")
        self.high_risk = QRadioButton("Высокий риск")
        self.low_risk.setChecked(True)

        # Кнопки
        self.default_btn = QPushButton("По умолчанию")
        self.apply_btn = QPushButton("Применить")
        self.default_btn.clicked.connect(self.set_defaults)
        self.apply_btn.clicked.connect(self.apply_profile)

        # Добавление в layout
        self.layout.addWidget(self.slider_label)
        self.layout.addWidget(self.slider)
        self.layout.addWidget(self.low_risk)
        self.layout.addWidget(self.mid_risk)
        self.layout.addWidget(self.high_risk)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.default_btn)
        btn_layout.addWidget(self.apply_btn)
        self.layout.addLayout(btn_layout)

    def update_slider_label(self, value):
        self.slider_label.setText(f"Уровень риска: {value}")

        if value <= 33:
            self.low_risk.setChecked(True)
        elif value <= 66:
            self.mid_risk.setChecked(True)
        else:
            self.high_risk.setChecked(True)

    def set_defaults(self):
        self.slider.setValue(0)
        self.low_risk.setChecked(True)

    def apply_profile(self):
        risk_value = self.slider.value()

        if risk_value <= 33:
            risk_level = "low"
            target = 1
        elif risk_value <= 66:
            risk_level = "medium"
            target = 2
        else:
            risk_level = "high"
            target = 3

        # Сохраняем глобально или в конфиг
        settings = Settings()
        settings.risk_level = risk_level
        settings.risk_value = risk_value
        settings.target_index = target
        settings.save()

        QMessageBox.information(self, "Применено", f"Установлен профиль: {risk_level.upper()} (TP{target})")

