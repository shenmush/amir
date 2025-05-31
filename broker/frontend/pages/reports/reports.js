window.usdPrices = {};

$(document).ready(function() {
    let usdPrices = {};
    loadUsdPrices();
    // راه‌اندازی Select2
    $("#marketFilterFactory, #producerFilter, #marketFilterProduct, #productFilter").select2({
      dir: "rtl",
      width: "220px",
      language: {
        noResults: function() { return "موردی یافت نشد"; }
      }
    });
  
    // سوییچ بین تب‌ها
    $("input[name='reportType']").on("change", function() {
      if ($("#tabFactory").is(":checked")) {
        $("#factoryFilters").show();
        $("#productFilters").hide();
        $("#reportTitle").text("گزارش تجمیعی فروش کارخانه");
        $("#monthlyChartContainer").html("");
        $("#reportTableContainer").html("");
        $("#filterSummary").html("");
        $(".selected-filters").hide();
      } else {
        $("#factoryFilters").hide();
        $("#productFilters").show();
        $("#reportTitle").text("گزارش تجمیعی فروش محصول");
        $("#monthlyChartContainer").html("");
        $("#reportTableContainer").html("");
        $("#filterSummary").html("");
        $(".selected-filters").hide();
      }
    });
  
    // بارگذاری لیست تالارها برای هر تب
    loadMarkets("#marketFilterFactory");
    loadMarkets("#marketFilterProduct");
  
    // بعد از انتخاب تالار، لیست کارخانه‌ها یا محصولات را بگیر
    $("#marketFilterFactory").on("change", function() {
      const market = $(this).val();
      if (market) {
        loadProducers(market, "#producerFilter");
      } else {
        $("#producerFilter").empty().append('<option value="">انتخاب کارخانه</option>');
      }
    });
    $("#marketFilterProduct").on("change", function() {
      const market = $(this).val();
      if (market) {
        loadProducts(market, "#productFilter");
      } else {
        $("#productFilter").empty().append('<option value="">انتخاب محصول</option>');
      }
    });
  
    // رویداد دکمه اعمال فیلترها (کارخانه)
    $("#applyFiltersFactory").on("click", async function() {
      const market = $("#marketFilterFactory").val();
      const producer = $("#producerFilter").val();
      if (!market || !producer) {
        alert("لطفاً تالار و کارخانه را انتخاب کنید.");
        return;
      }
      const url = new URL("http://185.213.164.220:8000/markets/data");
      url.searchParams.append("market", market);
      url.searchParams.append("producer", producer);
      const response = await fetch(url);
      const data = await response.json();
      if (data.filtered_data) {
        const grouped = groupByJalaliYearMonthProduct(data.filtered_data);
        showAccordionReportFactory(grouped);
        updateFilterSummary(`تالار: ${market}`, `کارخانه: ${producer}`);
      } else {
        $("#monthlyChartContainer").html("");
        $("#reportTableContainer").html("<p>داده‌ای برای نمایش وجود ندارد.</p>");
        updateFilterSummary(`تالار: ${market}`, `کارخانه: ${producer}`);
      }
    });
  
    // رویداد دکمه اعمال فیلترها (محصول)
    $("#applyFiltersProduct").on("click", async function() {
      const market = $("#marketFilterProduct").val();
      const product = $("#productFilter").val();
      if (!market || !product) {
        alert("لطفاً تالار و محصول را انتخاب کنید.");
        return;
      }
      const url = new URL("http://185.213.164.220:8000/markets/data");
      url.searchParams.append("market", market);
      url.searchParams.append("product_name", product);
      const response = await fetch(url);
      const data = await response.json();
      if (data.filtered_data) {
        const grouped = groupByJalaliYearMonthFactory(data.filtered_data);
        showAccordionReportByProduct(grouped);
        updateFilterSummary(`تالار: ${market}`, `محصول: ${product}`);
      } else {
        $("#monthlyChartContainer").html("");
        $("#reportTableContainer").html("<p>داده‌ای برای نمایش وجود ندارد.</p>");
        updateFilterSummary(`تالار: ${market}`, `محصول: ${product}`);
      }
    });
  
    // توابع کمکی
    function loadMarkets(selector) {
      $.get("http://185.213.164.220:8000/markets/data?unique=true", function(data) {
        if (data.markets) {
          const marketSelect = $(selector);
          marketSelect.empty().append('<option value="">انتخاب تالار</option>');
          data.markets.forEach(market => {
            marketSelect.append(new Option(market, market));
          });
        }
      });
    }
  
    function loadProducers(market, selector) {
      $.get(`http://185.213.164.220:8000/markets/data?market=${encodeURIComponent(market)}&unique=true`, function(data) {
        if (data.producers) {
          const producerSelect = $(selector);
          producerSelect.empty().append('<option value="">انتخاب کارخانه</option>');
          data.producers.forEach(producer => {
            producerSelect.append(new Option(producer, producer));
          });
        }
      });
    }
  
    function loadProducts(market, selector) {
      $.get(`http://185.213.164.220:8000/markets/data?market=${encodeURIComponent(market)}&unique=true`, function(data) {
        let products = [];
        if (data.products && Array.isArray(data.products)) {
          products = data.products;
        } else if (data.product_names && Array.isArray(data.product_names)) {
          products = data.product_names;
        }
        const productSelect = $(selector);
        productSelect.empty().append('<option value="">انتخاب محصول</option>');
        products.forEach(product => {
          productSelect.append(new Option(product, product));
        });
        productSelect.trigger("change");
      });
    }
  
    // گروه‌بندی داده‌ها به صورت سال و ماه شمسی ← محصولات (برای کارخانه)
    function groupByJalaliYearMonthProduct(data) {
      const result = {};
      data.forEach(item => {
        if (!item.transaction_date || !item.product_name) return;
        const m = moment(item.transaction_date);
        const year = m.format('jYYYY');
        const month = m.format('jM');
        const day = m.format('jD');
        const dateShamsi = normalizeDateShamsi(year, month, day);

        // لاگ دقیق
        console.log('transaction_date:', item.transaction_date, '| dateShamsi:', dateShamsi, '| exists:', usdPrices.hasOwnProperty(dateShamsi), '| نمونه کلید:', Object.keys(usdPrices).slice(0, 5));

        const value = (Number(item.transaction_value) || 0) * 1000;

        // نمونه کلیدهای usdPrices
        if (Object.keys(usdPrices).length && Math.random() < 0.01) { // فقط گاهی چاپ کن که کنسول شلوغ نشه
          console.log('نمونه کلیدهای usdPrices:', Object.keys(usdPrices).slice(0, 10));
        }

        // آیا کلید دقیقاً وجود دارد؟
        const hasExact = usdPrices.hasOwnProperty(dateShamsi);

        // اگر نبود، نزدیک‌ترین کلید بعدی چیست؟
        let nextKey = null;
        if (!hasExact) {
          const allDates = Object.keys(usdPrices).sort();
          for (let i = 0; i < allDates.length; i++) {
            if (allDates[i] > dateShamsi) {
              nextKey = allDates[i];
              break;
            }
          }
        }

        const usdPrice = getUsdPriceForDate(dateShamsi);
        const dollarValue = usdPrice ? value / usdPrice : null;

        console.log(
          "تاریخ شمسی:", dateShamsi,
          "| قیمت دلار:", usdPrice,
          "| ارزش معامله (ریال):", value,
          "| ارزش دلاری:", dollarValue,
          "| محصول:", item.product_name,
          "| کارخانه:", item.producer,
          "| حجم قرارداد:", item.contract_volume,
          "| تاریخ میلادی:", item.transaction_date
        );

        if (!result[year]) {
          result[year] = { total_value: 0, total_value_usd: 0, months: {} };
        }
        if (!result[year].months[month]) {
          result[year].months[month] = { total_value: 0, total_value_usd: 0, products: {} };
        }
        result[year].total_value += value;
        result[year].months[month].total_value += value;
        if (dollarValue) {
          result[year].total_value_usd += dollarValue;
          result[year].months[month].total_value_usd += dollarValue;
        }
        if (!result[year].months[month].products[item.product_name]) {
          result[year].months[month].products[item.product_name] = { total_volume: 0, total_value: 0, total_value_usd: 0 };
        }
        result[year].months[month].products[item.product_name].total_volume += Number(item.contract_volume) || 0;
        result[year].months[month].products[item.product_name].total_value += value;
        if (dollarValue) {
          result[year].months[month].products[item.product_name].total_value_usd += dollarValue;
        }
      });
      return result;
    }
  
    // گروه‌بندی داده‌ها به صورت سال و ماه شمسی ← کارخانه‌ها (برای محصول)
    function groupByJalaliYearMonthFactory(data) {
      const result = {};
      data.forEach(item => {
        if (!item.transaction_date || !item.producer) return;
        const m = moment(item.transaction_date);
        const year = m.format('jYYYY');
        const month = m.format('jMM');
        const day = m.format('jD');
        const dateShamsi = normalizeDateShamsi(year, month, day);
        const value = (Number(item.transaction_value) || 0) * 1000;
        const usdPrice = getUsdPriceForDate(dateShamsi);
        const dollarValue = usdPrice ? value / usdPrice : null;

        if (!result[year]) {
          result[year] = { total_value: 0, total_value_usd: 0, months: {} };
        }
        if (!result[year].months[month]) {
          result[year].months[month] = { total_value: 0, total_value_usd: 0, factories: {} };
        }
        result[year].total_value += value;
        result[year].months[month].total_value += value;
        if (dollarValue) {
          result[year].total_value_usd += dollarValue;
          result[year].months[month].total_value_usd += dollarValue;
        }

        const factory = item.producer;
        if (!result[year].months[month].factories[factory]) {
          result[year].months[month].factories[factory] = { 
            total_volume: 0, 
            total_value: 0,
            total_value_usd: 0 
          };
        }
        result[year].months[month].factories[factory].total_volume += Number(item.contract_volume) || 0;
        result[year].months[month].factories[factory].total_value += value;
        if (dollarValue) {
          result[year].months[month].factories[factory].total_value_usd += dollarValue;
    }
  });
      return result;
    }
  
    // داده مناسب برای نمودار (ماهانه)
    function getMonthlyChartData(groupedData) {
      let chartDataRial = [];
      let chartDataDollar = [];
      let categories = [];
      Object.keys(groupedData).sort().forEach(year => {
        Object.keys(groupedData[year].months).sort().forEach(month => {
          const label = year + " " + getPersianMonthName(month);
          categories.push(label);
          chartDataRial.push(groupedData[year].months[month].total_value);
          chartDataDollar.push(groupedData[year].months[month].total_value_usd || 0);
        });
      });
      return { categories, chartDataRial, chartDataDollar };
    }
  
    // جدول و نمودار آکاردئونی برای کارخانه
    function showAccordionReportFactory(groupedData) {
      const { categories, chartDataRial, chartDataDollar } = getMonthlyChartData(groupedData);
      Highcharts.chart('monthlyChartContainer', {
        chart: { style: { fontFamily: 'Vazirmatn' } },
        title: { text: 'نمودار فروش ماهانه کارخانه', style: { fontSize: '18px', fontWeight: 'bold' } },
        xAxis: { categories, title: { text: 'ماه' } },
        yAxis: [{
          min: 0,
          title: { text: 'ارزش معاملات (ریال)' },
          labels: { formatter: function() { return toPersianDigits(this.value); } }
        }, {
          min: 0,
          title: { text: 'ارزش معاملات (دلار)' },
          labels: { formatter: function() { return toPersianDigits(this.value); } },
          opposite: true
        }],
        series: [
          {
            name: 'ارزش معاملات (ریال)',
            type: 'column',
            data: chartDataRial,
            color: '#2196f3',
            yAxis: 0,
            tooltip: { valueSuffix: ' ریال' }
          },
          {
            name: 'ارزش معاملات (دلار)',
            type: 'line',
            data: chartDataDollar,
            color: '#e53935',
            yAxis: 1,
            tooltip: { valueSuffix: ' دلار' }
          }
        ],
        tooltip: { shared: true },
        credits: { enabled: false }
      });
  
      let html = `<table class="data-table" id="reportTable">
          <thead>
            <tr>
            <th>سال</th>
              <th>ماه</th>
            <th>محصول</th>
              <th>مقدار عرضه</th>
            <th>ارزش معاملات <span style="font-size:13px; color:#888;">(ریال)</span></th>
            <th>ارزش معاملات دلاری <span style="font-size:13px; color:#888;">(USD)</span></th>
            </tr>
          </thead>
          <tbody>
      `;
      const years = Object.keys(groupedData).sort((a, b) => b - a);
      years.forEach(year => {
        html += `
          <tr class="year-row" data-year="${year}">
            <td>
              <span class="toggle-icon">&#9654;</span>
              <b>${toPersianDigits(year)}</b>
            </td>
            <td colspan="4"><b>ارزش کل فروش: <span class="numeric">${toPersianDigits(groupedData[year].total_value.toLocaleString('fa-IR'))}</span> <span style="font-size:13px; color:#888;">ریال</span></b></td>
            <td>${toPersianDigits(groupedData[year].total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2}))}</td>
          </tr>
        `;
        const months = Object.keys(groupedData[year].months).sort((a, b) => b - a);
        months.forEach(month => {
          html += `
            <tr class="month-row" data-year="${year}" data-month="${month}" style="display:none;">
              <td></td>
              <td>
                <span class="toggle-icon">&#9654;</span>
                <b>${getPersianMonthName(month)}</b>
              </td>
              <td colspan="3"><b>ارزش کل فروش ماه: <span class="numeric">${toPersianDigits(groupedData[year].months[month].total_value.toLocaleString('fa-IR'))}</span> <span style="font-size:13px; color:#888;">ریال</span></b></td>
              <td>${toPersianDigits(groupedData[year].months[month].total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2}))}</td>
            </tr>
          `;
          const products = Object.entries(groupedData[year].months[month].products);
          products.forEach(([product, info]) => {
            html += `
              <tr class="product-row" data-year="${year}" data-month="${month}" style="display:none;">
                <td></td>
                <td></td>
                <td>${product}</td>
                <td>${toPersianDigits(info.total_volume.toLocaleString('fa-IR'))}</td>
                <td>${toPersianDigits(info.total_value.toLocaleString('fa-IR'))}</td>
                <td>${info.total_value_usd ? toPersianDigits(info.total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2})) : '-'}</td>
              </tr>
            `;
          });
        });
      });
      html += "</tbody></table>";
      $("#reportTableContainer").html(html);
  
      $(".year-row").on("click", function() {
        const year = $(this).data("year");
        $(this).toggleClass("open");
        $(`.month-row[data-year='${year}']`).slideToggle(200);
        $(`.product-row[data-year='${year}']`).hide();
      });
      $(".month-row").on("click", function(e) {
        e.stopPropagation();
        const year = $(this).data("year");
        const month = $(this).data("month");
        $(this).toggleClass("open");
        $(`.product-row[data-year='${year}'][data-month='${month}']`).slideToggle(200);
      });
    }
  
    // جدول و نمودار آکاردئونی برای محصول
    function showAccordionReportByProduct(groupedData) {
      const { categories, chartDataRial, chartDataDollar } = getMonthlyChartData(groupedData);
      Highcharts.chart('monthlyChartContainer', {
        chart: { style: { fontFamily: 'Vazirmatn' } },
        title: { text: 'نمودار فروش ماهانه محصول', style: { fontSize: '18px', fontWeight: 'bold' } },
        xAxis: { categories, title: { text: 'ماه' } },
        yAxis: [{
          min: 0,
          title: { text: 'ارزش معاملات (ریال)' },
          labels: { formatter: function() { return toPersianDigits(this.value); } }
        }, {
          min: 0,
          title: { text: 'ارزش معاملات (دلار)' },
          labels: { formatter: function() { return toPersianDigits(this.value); } },
          opposite: true
        }],
        series: [
          {
            name: 'ارزش معاملات (ریال)',
            type: 'column',
            data: chartDataRial,
            color: '#2196f3',
            yAxis: 0,
            tooltip: { valueSuffix: ' ریال' }
          },
          {
            name: 'ارزش معاملات (دلار)',
            type: 'line',
            data: chartDataDollar,
            color: '#e53935',
            yAxis: 1,
            tooltip: { valueSuffix: ' دلار' }
          }
        ],
        tooltip: { shared: true },
        credits: { enabled: false }
      });
  
      let html = `<table class="data-table" id="reportTable">
        <thead>
          <tr>
            <th>سال</th>
            <th>ماه</th>
            <th>کارخانه</th>
            <th>مقدار عرضه</th>
            <th>ارزش معاملات <span style="font-size:13px; color:#888;">(ریال)</span></th>
            <th>ارزش معاملات دلاری <span style="font-size:13px; color:#888;">(USD)</span></th>
          </tr>
        </thead>
        <tbody>
      `;
      const years = Object.keys(groupedData).sort((a, b) => b - a);
      years.forEach(year => {
        html += `
          <tr class="year-row" data-year="${year}">
            <td>
              <span class="toggle-icon">&#9654;</span>
              <b>${toPersianDigits(year)}</b>
            </td>
            <td colspan="4"><b>ارزش کل فروش: <span class="numeric">${toPersianDigits(groupedData[year].total_value.toLocaleString('fa-IR'))}</span> <span style="font-size:13px; color:#888;">ریال</span></b></td>
            <td>${groupedData[year].total_value_usd ? toPersianDigits(groupedData[year].total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2})) : '-'}</td>
          </tr>
        `;
        const months = Object.keys(groupedData[year].months).sort((a, b) => b - a);
        months.forEach(month => {
          html += `
            <tr class="month-row" data-year="${year}" data-month="${month}" style="display:none;">
              <td></td>
              <td>
                <span class="toggle-icon">&#9654;</span>
                <b>${getPersianMonthName(month)}</b>
              </td>
              <td colspan="3"><b>ارزش کل فروش ماه: <span class="numeric">${toPersianDigits(groupedData[year].months[month].total_value.toLocaleString('fa-IR'))}</span> <span style="font-size:13px; color:#888;">ریال</span></b></td>
              <td>${groupedData[year].months[month].total_value_usd ? toPersianDigits(groupedData[year].months[month].total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2})) : '-'}</td>
            </tr>
          `;
          const factories = Object.entries(groupedData[year].months[month].factories)
            .sort((a, b) => b[1].total_value - a[1].total_value);
          factories.forEach(([factory, info]) => {
            html += `
              <tr class="factory-row" data-year="${year}" data-month="${month}" style="display:none;">
                <td></td>
                <td></td>
                <td>${factory}</td>
                <td>${toPersianDigits(info.total_volume.toLocaleString('fa-IR'))}</td>
                <td>${toPersianDigits(info.total_value.toLocaleString('fa-IR'))}</td>
                <td>${info.total_value_usd ? toPersianDigits(info.total_value_usd.toLocaleString('fa-IR', {maximumFractionDigits: 2})) : '-'}</td>
              </tr>
            `;
          });
        });
      });
      html += "</tbody></table>";
      $("#reportTableContainer").html(html);
  
      $(".year-row").on("click", function() {
        const year = $(this).data("year");
        $(this).toggleClass("open");
        $(`.month-row[data-year='${year}']`).slideToggle(200);
        $(`.factory-row[data-year='${year}']`).hide();
      });
      $(".month-row").on("click", function(e) {
        e.stopPropagation();
        const year = $(this).data("year");
        const month = $(this).data("month");
        $(this).toggleClass("open");
        $(`.factory-row[data-year='${year}'][data-month='${month}']`).slideToggle(200);
      });
    }
  
    function updateFilterSummary(...args) {
      let summaryText = args.map(x => `<span>${x}</span>`).join('');
      $("#filterSummary").html(summaryText || "هیچ فیلتری انتخاب نشده است");
      if (summaryText) {
        $(".selected-filters").show();
      } else {
        $(".selected-filters").hide();
      }
    }

    function toPersianDigits(str) {
      return String(str).replace(/\d/g, d => '۰۱۲۳۴۵۶۷۸۹'[d]);
    }

    const persianMonths = [
      '', 'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
      'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
    ];

    // تبدیل عدد ماه (مثلاً "01") به نام ماه فارسی
    function getPersianMonthName(monthNum) {
      return persianMonths[parseInt(monthNum, 10)];
    }

    // ۱. خواندن قیمت دلار
    function loadUsdPrices(callback) {
      fetch('http://185.213.164.220:8000/usd-prices')
        .then(res => res.json())
        .then(data => {
          window.usdPrices = data;
          console.log('usdPrices loaded:', Object.keys(window.usdPrices).length, 'نمونه:', Object.keys(window.usdPrices).slice(0, 10));
          if (callback) callback();
        });
    }

    // ۲. تابع پیدا کردن قیمت دلار
    function getUsdPriceForDate(date) {
      if (window.usdPrices[date]) return window.usdPrices[date];
      const allDates = Object.keys(window.usdPrices).sort();
      let lastPrice = null;
      for (let i = 0; i < allDates.length; i++) {
        if (allDates[i] > date) {
          break;
        }
        lastPrice = window.usdPrices[allDates[i]];
      }
      return lastPrice;
    }

    function normalizeDateShamsi(y, m, d) {
      // اطمینان از اعداد انگلیسی و اسلش انگلیسی و اضافه کردن صفر پیشوند
      m = String(m).padStart(2, '0');
      d = String(d).padStart(2, '0');
      return [y, m, d].map(x => String(x).replace(/[۰-۹]/g, d => '0123456789'['۰۱۲۳۴۵۶۷۸۹'.indexOf(d)])).join('/');
    }
  });