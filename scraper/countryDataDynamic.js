import puppeteer from "puppeteer";
import config from "../config/index.js";
import { parseNumber, parsePercentage } from "./utils.js";

const COUNTRY_NAME_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  "Iran (Islamic Republic of)": "Iran",
  "Viet Nam": "Vietnam",
  Czechia: "Czech Republic",
};

// Yeni sıralama fonksiyonu
const applySorting = async (page) => {
  await page.waitForSelector('#example2 thead th[data-sortable="true"]', {
    timeout: 30000,
  });

  // Daha spesifik sıralama butonu seçici
  const sortHeader = await page.$x(
    '//th[contains(., "Population") or contains(., "Nüfus")]/button[contains(@class, "datatable-sorter")]'
  );

  if (!sortHeader.length) {
    throw new Error("Population sıralama başlığı bulunamadı");
  }

  // Sıralama durum kontrolü
  const isAlreadySorted = await page.evaluate((header) => {
    return header.parentElement.classList.contains("datatable-descending");
  }, sortHeader[0]);

  if (!isAlreadySorted) {
    await sortHeader[0].click();
    await page.waitForNetworkIdle({ idleTime: 1000 });
  }
};

export const fetchCountryDataDynamic = async () => {
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    const page = await browser.newPage();

    // Gerçekçi User-Agent ve geniş viewport ayarı
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    );
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setDefaultNavigationTimeout(45000);

    console.log(`Navigating to country URL: ${config.COUNTRY_URL}`);
    await page.goto(config.COUNTRY_URL, {
      waitUntil: "networkidle2",
      timeout: 60000,
    });

    // Sayfa içeriğinin tamamen yüklenip, tablo satır sayısının 1'den fazla olmasını bekleyelim.
    await page.waitForFunction(
      () => document.querySelectorAll("#example2 tbody tr").length > 0,
      { timeout: 45000 }
    );

    // Debug: Sayfa içeriğini inceleyip doğru elemanın yüklenip yüklenmediğini kontrol edebilirsiniz.
    // const pageContent = await page.content();
    // console.log(pageContent);

    // XPath ile sort butonunu bulup tıklama işlemleri
    const sortButtons = await page.$x(
      '//th[contains(@aria-label, "activate to sort column descending")]'
    );
    if (!sortButtons.length) {
      throw new Error("Sort button not found");
    }
    const sortButton = sortButtons[0];

    console.log("Applying population sorting...");
    await sortButton.click(); // İlk tıklama (asc)
    await page.waitForTimeout(2000);
    await sortButton.click(); // İkinci tıklama (desc)

    // Sıralamadan sonra tablo satır sayısının güncellendiğini bekleyelim.
    await page.waitForFunction(
      () => document.querySelectorAll("#example2 tbody tr").length > 0,
      { timeout: 45000 }
    );

    // Daha sağlam veri çekme yöntemi
    const countryData = await page.evaluate((mapping) => {
      return Array.from(document.querySelectorAll("#example2 tbody tr")).map(
        (row) => {
          const cells = row.querySelectorAll("td");
          const getData = (index, type = "text") => {
            const cell = cells[index];
            return type === "number"
              ? parseFloat(
                  cell.dataset.order || cell.innerText.replace(/[^\d.-]/g, "")
                )
              : cell.textContent.trim();
          };

          return {
            country: mapping[getData(1)] || getData(1),
            raw_values: {
              population: getData(2, "number"),
              yearly_change: getData(3, "number"),
              net_change: getData(4, "number"),
              migrants: getData(7, "number"),
              med_age: getData(9, "number"),
            },
          };
        }
      );
    }, COUNTRY_NAME_MAPPING);

    // Veri parsing işlemleri
    const parsedData = countryData
      .map((item) => {
        const parsed = {
          country: item.country,
          current_population: parseNumber(item.raw_values.population),
          yearly_change: parsePercentage(item.raw_values.yearly_change),
          net_change: parseNumber(item.raw_values.net_change),
          migrants: parseNumber(item.raw_values.migrants),
          med_age: parseNumber(item.raw_values.med_age),
          "@timestamp": new Date().toISOString(),
          type: "country",
          is_current: true,
        };

        if (Object.values(parsed).some((v) => v === null)) {
          console.warn(`Invalid data for ${item.country}`);
          return null;
        }

        return parsed;
      })
      .filter(Boolean);

    console.log(`Successfully parsed ${parsedData.length} countries`);
    return parsedData;
  } catch (error) {
    console.error("Country scraping error:", error.message);
    return null;
  } finally {
    if (browser) await browser.close();
  }
};
