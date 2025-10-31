package market

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Data 市场数据结构
type Data struct {
	Symbol            string
	CurrentPrice      float64
	PriceChange1h     float64 // 1小时价格变化百分比
	PriceChange4h     float64 // 4小时价格变化百分比
	CurrentEMA20      float64
	CurrentMACD       float64
	CurrentRSI7       float64
	OpenInterest      *OIData
	Funding           *FundingData
	Timeframes        map[string]*TimeframeMetrics
	Microstructure    *MicrostructureData
	IntradaySeries    *IntradayData
	LongerTermContext *LongerTermData
}

// FundingData 资金费率与斜率数据
type FundingData struct {
	Rate       float64
	Slope      float64
	NextTimeMs int64
}

// OIData Open Interest数据
type OIData struct {
	Latest        float64
	Average       float64
	Delta5m       float64
	Delta15m      float64
	Delta1h       float64
	Delta4h       float64
	PriceDelta5m  float64
	PriceDelta15m float64
	PriceDelta1h  float64
	PriceDelta4h  float64
	TimestampMs   int64
}

// TimeframeMetrics 多周期指标
type TimeframeMetrics struct {
	Interval       string
	Close          float64
	RSI7           float64
	RSI14          float64
	MACD           float64
	EMA20          float64
	EMA60          float64
	BollingerWidth float64
	ATR14          float64
	RealizedVol20  float64
	CurrentVolume  float64
	AverageVolume  float64
}

// MicrostructureData 微结构指标
type MicrostructureData struct {
	CVD1m      float64
	CVD3m      float64
	CVD15m     float64
	OFI1m      float64
	OFI3m      float64
	OFI15m     float64
	OBI10      float64
	MicroPrice float64
}

// IntradayData 日内数据(3分钟间隔)
type IntradayData struct {
	MidPrices   []float64
	EMA20Values []float64
	MACDValues  []float64
	RSI7Values  []float64
	RSI14Values []float64
}

// LongerTermData 长期数据(4小时时间框架)
type LongerTermData struct {
	EMA20         float64
	EMA50         float64
	ATR3          float64
	ATR14         float64
	CurrentVolume float64
	AverageVolume float64
	MACDValues    []float64
	RSI14Values   []float64
}

// Kline K线数据
type Kline struct {
	OpenTime  int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	CloseTime int64
}

// Get 获取指定代币的市场数据
func Get(symbol string) (*Data, error) {
	// 标准化symbol
	symbol = Normalize(symbol)

	intervals := []string{"1m", "3m", "15m", "1h", "4h"}
	klinesByInterval := make(map[string][]Kline, len(intervals))

	for _, interval := range intervals {
		limit := 200
		if interval == "4h" {
			limit = 120
		}
		klines, err := getKlines(symbol, interval, limit)
		if err != nil {
			return nil, fmt.Errorf("获取%s K线失败: %v", interval, err)
		}
		klinesByInterval[interval] = klines
	}

	// 基准使用3分钟周期
	klines3m := klinesByInterval["3m"]
	currentPrice := klines3m[len(klines3m)-1].Close

	timeframeMetrics := make(map[string]*TimeframeMetrics, len(intervals))
	for _, interval := range intervals {
		metrics := calculateTimeframeMetrics(interval, klinesByInterval[interval])
		timeframeMetrics[interval] = metrics
	}

	currentEMA20 := timeframeMetrics["3m"].EMA20
	currentMACD := timeframeMetrics["3m"].MACD
	currentRSI7 := timeframeMetrics["3m"].RSI7

	priceChange1h := percentageChangeFromSeries(klinesByInterval["1m"], 60)
	priceChange4h := percentageChangeFromSeries(klinesByInterval["1h"], 4)

	oiData, err := getOpenInterestData(symbol,
		klinesByInterval["1m"],
		klinesByInterval["15m"],
		klinesByInterval["1h"],
		klinesByInterval["4h"],
	)
	if err != nil {
		oiData = &OIData{}
	}

	fundingData, _ := getFundingData(symbol)

	microstructure := getMicrostructureData(symbol)

	intradayData := calculateIntradaySeries(klines3m)
	longerTermData := calculateLongerTermData(klinesByInterval["4h"])

	return &Data{
		Symbol:            symbol,
		CurrentPrice:      currentPrice,
		PriceChange1h:     priceChange1h,
		PriceChange4h:     priceChange4h,
		CurrentEMA20:      currentEMA20,
		CurrentMACD:       currentMACD,
		CurrentRSI7:       currentRSI7,
		OpenInterest:      oiData,
		Funding:           fundingData,
		Timeframes:        timeframeMetrics,
		Microstructure:    microstructure,
		IntradaySeries:    intradayData,
		LongerTermContext: longerTermData,
	}, nil
}

// getKlines 从Binance获取K线数据
func getKlines(symbol, interval string, limit int) ([]Kline, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		symbol, interval, limit)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rawData [][]interface{}
	if err := json.Unmarshal(body, &rawData); err != nil {
		return nil, err
	}

	klines := make([]Kline, len(rawData))
	for i, item := range rawData {
		openTime := int64(item[0].(float64))
		open, _ := parseFloat(item[1])
		high, _ := parseFloat(item[2])
		low, _ := parseFloat(item[3])
		close, _ := parseFloat(item[4])
		volume, _ := parseFloat(item[5])
		closeTime := int64(item[6].(float64))

		klines[i] = Kline{
			OpenTime:  openTime,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			CloseTime: closeTime,
		}
	}

	return klines, nil
}

// calculateEMA 计算EMA
func calculateEMA(klines []Kline, period int) float64 {
	if len(klines) < period {
		return 0
	}

	// 计算SMA作为初始EMA
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += klines[i].Close
	}
	ema := sum / float64(period)

	// 计算EMA
	multiplier := 2.0 / float64(period+1)
	for i := period; i < len(klines); i++ {
		ema = (klines[i].Close-ema)*multiplier + ema
	}

	return ema
}

// calculateMACD 计算MACD
func calculateMACD(klines []Kline) float64 {
	if len(klines) < 26 {
		return 0
	}

	// 计算12期和26期EMA
	ema12 := calculateEMA(klines, 12)
	ema26 := calculateEMA(klines, 26)

	// MACD = EMA12 - EMA26
	return ema12 - ema26
}

// calculateRSI 计算RSI
func calculateRSI(klines []Kline, period int) float64 {
	if len(klines) <= period {
		return 0
	}

	gains := 0.0
	losses := 0.0

	// 计算初始平均涨跌幅
	for i := 1; i <= period; i++ {
		change := klines[i].Close - klines[i-1].Close
		if change > 0 {
			gains += change
		} else {
			losses += -change
		}
	}

	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)

	// 使用Wilder平滑方法计算后续RSI
	for i := period + 1; i < len(klines); i++ {
		change := klines[i].Close - klines[i-1].Close
		if change > 0 {
			avgGain = (avgGain*float64(period-1) + change) / float64(period)
			avgLoss = (avgLoss * float64(period-1)) / float64(period)
		} else {
			avgGain = (avgGain * float64(period-1)) / float64(period)
			avgLoss = (avgLoss*float64(period-1) + (-change)) / float64(period)
		}
	}

	if avgLoss == 0 {
		return 100
	}

	rs := avgGain / avgLoss
	rsi := 100 - (100 / (1 + rs))

	return rsi
}

// calculateATR 计算ATR
func calculateATR(klines []Kline, period int) float64 {
	if len(klines) <= period {
		return 0
	}

	trs := make([]float64, len(klines))
	for i := 1; i < len(klines); i++ {
		high := klines[i].High
		low := klines[i].Low
		prevClose := klines[i-1].Close

		tr1 := high - low
		tr2 := math.Abs(high - prevClose)
		tr3 := math.Abs(low - prevClose)

		trs[i] = math.Max(tr1, math.Max(tr2, tr3))
	}

	// 计算初始ATR
	sum := 0.0
	for i := 1; i <= period; i++ {
		sum += trs[i]
	}
	atr := sum / float64(period)

	// Wilder平滑
	for i := period + 1; i < len(klines); i++ {
		atr = (atr*float64(period-1) + trs[i]) / float64(period)
	}

	return atr
}

func calculateBollingerWidth(klines []Kline, period int, multiplier float64) float64 {
	if len(klines) < period {
		return 0
	}

	closes := make([]float64, period)
	for i := 0; i < period; i++ {
		closes[i] = klines[len(klines)-period+i].Close
	}

	mean := 0.0
	for _, v := range closes {
		mean += v
	}
	mean /= float64(period)

	variance := 0.0
	for _, v := range closes {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(period)

	stddev := math.Sqrt(variance)
	if mean == 0 {
		return 0
	}

	upper := mean + multiplier*stddev
	lower := mean - multiplier*stddev
	width := (upper - lower) / mean
	return width
}

func calculateRealizedVol(klines []Kline, period int) float64 {
	if len(klines) <= period {
		return 0
	}

	returns := make([]float64, 0, period)
	for i := len(klines) - period; i < len(klines); i++ {
		if i == 0 {
			continue
		}
		prev := klines[i-1].Close
		if prev <= 0 {
			continue
		}
		r := math.Log(klines[i].Close / prev)
		returns = append(returns, r)
	}

	if len(returns) == 0 {
		return 0
	}

	mean := 0.0
	for _, v := range returns {
		mean += v
	}
	mean /= float64(len(returns))

	variance := 0.0
	for _, v := range returns {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(returns))

	return math.Sqrt(variance)
}

func calculateAverageVolume(klines []Kline, period int) (float64, float64) {
	if len(klines) == 0 {
		return 0, 0
	}

	current := klines[len(klines)-1].Volume

	if len(klines) < period {
		sum := 0.0
		for _, k := range klines {
			sum += k.Volume
		}
		return current, sum / float64(len(klines))
	}

	sum := 0.0
	for i := len(klines) - period; i < len(klines); i++ {
		sum += klines[i].Volume
	}

	return current, sum / float64(period)
}

func calculateTimeframeMetrics(interval string, klines []Kline) *TimeframeMetrics {
	metrics := &TimeframeMetrics{Interval: interval}
	if len(klines) == 0 {
		return metrics
	}

	metrics.Close = klines[len(klines)-1].Close
	metrics.RSI7 = calculateRSI(klines, 7)
	metrics.RSI14 = calculateRSI(klines, 14)
	metrics.MACD = calculateMACD(klines)
	metrics.EMA20 = calculateEMA(klines, 20)
	metrics.EMA60 = calculateEMA(klines, 60)
	metrics.BollingerWidth = calculateBollingerWidth(klines, 20, 2)
	metrics.ATR14 = calculateATR(klines, 14)
	metrics.RealizedVol20 = calculateRealizedVol(klines, 20)
	metrics.CurrentVolume, metrics.AverageVolume = calculateAverageVolume(klines, 20)
	return metrics
}

func percentageChangeFromSeries(klines []Kline, barsBack int) float64 {
	if len(klines) == 0 || barsBack <= 0 {
		return 0
	}

	if len(klines) <= barsBack {
		return 0
	}

	latest := klines[len(klines)-1].Close
	reference := klines[len(klines)-1-barsBack].Close
	if reference == 0 {
		return 0
	}

	return ((latest - reference) / reference) * 100
}

func priceDeltaFromKlines(klines []Kline, barsBack int) float64 {
	if len(klines) == 0 || barsBack <= 0 {
		return 0
	}

	if len(klines) <= barsBack {
		return 0
	}

	latest := klines[len(klines)-1].Close
	reference := klines[len(klines)-1-barsBack].Close
	return latest - reference
}

// calculateIntradaySeries 计算日内系列数据
func calculateIntradaySeries(klines []Kline) *IntradayData {
	data := &IntradayData{
		MidPrices:   make([]float64, 0, 10),
		EMA20Values: make([]float64, 0, 10),
		MACDValues:  make([]float64, 0, 10),
		RSI7Values:  make([]float64, 0, 10),
		RSI14Values: make([]float64, 0, 10),
	}

	// 获取最近10个数据点
	start := len(klines) - 10
	if start < 0 {
		start = 0
	}

	for i := start; i < len(klines); i++ {
		data.MidPrices = append(data.MidPrices, klines[i].Close)

		// 计算每个点的EMA20
		if i >= 19 {
			ema20 := calculateEMA(klines[:i+1], 20)
			data.EMA20Values = append(data.EMA20Values, ema20)
		}

		// 计算每个点的MACD
		if i >= 25 {
			macd := calculateMACD(klines[:i+1])
			data.MACDValues = append(data.MACDValues, macd)
		}

		// 计算每个点的RSI
		if i >= 7 {
			rsi7 := calculateRSI(klines[:i+1], 7)
			data.RSI7Values = append(data.RSI7Values, rsi7)
		}
		if i >= 14 {
			rsi14 := calculateRSI(klines[:i+1], 14)
			data.RSI14Values = append(data.RSI14Values, rsi14)
		}
	}

	return data
}

// calculateLongerTermData 计算长期数据
func calculateLongerTermData(klines []Kline) *LongerTermData {
	data := &LongerTermData{
		MACDValues:  make([]float64, 0, 10),
		RSI14Values: make([]float64, 0, 10),
	}

	// 计算EMA
	data.EMA20 = calculateEMA(klines, 20)
	data.EMA50 = calculateEMA(klines, 50)

	// 计算ATR
	data.ATR3 = calculateATR(klines, 3)
	data.ATR14 = calculateATR(klines, 14)

	// 计算成交量
	if len(klines) > 0 {
		data.CurrentVolume = klines[len(klines)-1].Volume
		// 计算平均成交量
		sum := 0.0
		for _, k := range klines {
			sum += k.Volume
		}
		data.AverageVolume = sum / float64(len(klines))
	}

	// 计算MACD和RSI序列
	start := len(klines) - 10
	if start < 0 {
		start = 0
	}

	for i := start; i < len(klines); i++ {
		if i >= 25 {
			macd := calculateMACD(klines[:i+1])
			data.MACDValues = append(data.MACDValues, macd)
		}
		if i >= 14 {
			rsi14 := calculateRSI(klines[:i+1], 14)
			data.RSI14Values = append(data.RSI14Values, rsi14)
		}
	}

	return data
}

// getOpenInterestData 获取OI数据
type oiHistoryPoint struct {
	Value     float64
	Timestamp int64
}

func getOpenInterestData(symbol string, klines1m, klines15m, klines1h, klines4h []Kline) (*OIData, error) {
	latest, ts, err := getLatestOpenInterest(symbol)
	if err != nil {
		return nil, err
	}

	history5m, _ := getOpenInterestHistory(symbol, "5m", 20)
	history15m, _ := getOpenInterestHistory(symbol, "15m", 20)
	history1h, _ := getOpenInterestHistory(symbol, "1h", 20)
	history4h, _ := getOpenInterestHistory(symbol, "4h", 20)

	avg := latest
	if len(history4h) > 0 {
		sum := 0.0
		for _, pt := range history4h {
			sum += pt.Value
		}
		avg = sum / float64(len(history4h))
	}

	data := &OIData{
		Latest:      latest,
		Average:     avg,
		TimestampMs: ts,
	}

	if len(history5m) >= 2 {
		data.Delta5m = history5m[len(history5m)-1].Value - history5m[len(history5m)-2].Value
		data.PriceDelta5m = priceDeltaFromKlines(klines1m, 5)
	}

	if len(history15m) >= 2 {
		data.Delta15m = history15m[len(history15m)-1].Value - history15m[len(history15m)-2].Value
		data.PriceDelta15m = priceDeltaFromKlines(klines15m, 1)
	}

	if len(history1h) >= 2 {
		data.Delta1h = history1h[len(history1h)-1].Value - history1h[len(history1h)-2].Value
		data.PriceDelta1h = priceDeltaFromKlines(klines1h, 1)
	}

	if len(history4h) >= 2 {
		data.Delta4h = history4h[len(history4h)-1].Value - history4h[len(history4h)-2].Value
		data.PriceDelta4h = priceDeltaFromKlines(klines4h, 1)
	}

	return data, nil
}

func getLatestOpenInterest(symbol string) (float64, int64, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/openInterest?symbol=%s", symbol)

	resp, err := http.Get(url)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	var result struct {
		OpenInterest string `json:"openInterest"`
		Symbol       string `json:"symbol"`
		Time         int64  `json:"time"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, 0, err
	}

	oi, err := strconv.ParseFloat(result.OpenInterest, 64)
	if err != nil {
		return 0, 0, err
	}

	return oi, result.Time, nil
}

func getOpenInterestHistory(symbol, period string, limit int) ([]oiHistoryPoint, error) {
	url := fmt.Sprintf("https://fapi.binance.com/futures/data/openInterestHist?symbol=%s&period=%s&limit=%d", symbol, period, limit)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw []struct {
		Symbol          string `json:"symbol"`
		SumOpenInterest string `json:"sumOpenInterest"`
		Timestamp       int64  `json:"timestamp"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	points := make([]oiHistoryPoint, 0, len(raw))
	for _, item := range raw {
		value, err := strconv.ParseFloat(item.SumOpenInterest, 64)
		if err != nil {
			continue
		}
		points = append(points, oiHistoryPoint{Value: value, Timestamp: item.Timestamp})
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	return points, nil
}

// getFundingData 获取资金费率及变化斜率
type fundingRatePoint struct {
	Rate      float64
	Timestamp int64
}

func getFundingData(symbol string) (*FundingData, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=%s", symbol)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	rate, err := strconv.ParseFloat(result.LastFundingRate, 64)
	if err != nil {
		rate = 0
	}

	history, err := getFundingRateHistory(symbol, 8)
	if err != nil {
		history = nil
	}

	slope := 0.0
	if len(history) >= 2 {
		first := history[0]
		last := history[len(history)-1]
		duration := float64(last.Timestamp-first.Timestamp) / float64(time.Hour/time.Millisecond)
		if duration != 0 {
			slope = (last.Rate - first.Rate) / duration
		}
	}

	return &FundingData{
		Rate:       rate,
		Slope:      slope,
		NextTimeMs: result.NextFundingTime,
	}, nil
}

func getFundingRateHistory(symbol string, limit int) ([]fundingRatePoint, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/fundingRate?symbol=%s&limit=%d", symbol, limit)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw []struct {
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	points := make([]fundingRatePoint, 0, len(raw))
	for _, item := range raw {
		rate, err := strconv.ParseFloat(item.FundingRate, 64)
		if err != nil {
			continue
		}
		points = append(points, fundingRatePoint{Rate: rate, Timestamp: item.FundingTime})
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	return points, nil
}

// Format 格式化输出市场数据
func Format(data *Data) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("current_price = %.2f, current_ema20 = %.3f, current_macd = %.3f, current_rsi (7 period) = %.3f\n\n",
		data.CurrentPrice, data.CurrentEMA20, data.CurrentMACD, data.CurrentRSI7))

	sb.WriteString(fmt.Sprintf("In addition, here is the latest %s open interest and funding rate for perps:\n\n",
		data.Symbol))

	if data.OpenInterest != nil {
		sb.WriteString(fmt.Sprintf("Open Interest: Latest: %.2f Average: %.2f\n\n",
			data.OpenInterest.Latest, data.OpenInterest.Average))
	}

	if data.Funding != nil {
		sb.WriteString(fmt.Sprintf("Funding Rate: %.2e | Slope (per hour): %.2e | Next: %d\n\n",
			data.Funding.Rate, data.Funding.Slope, data.Funding.NextTimeMs))
	}

	if data.OpenInterest != nil {
		sb.WriteString(fmt.Sprintf("OI Δ (5m/15m/1h/4h): %.2f / %.2f / %.2f / %.2f | Price Δ: %.4f / %.4f / %.4f / %.4f\n\n",
			data.OpenInterest.Delta5m, data.OpenInterest.Delta15m, data.OpenInterest.Delta1h, data.OpenInterest.Delta4h,
			data.OpenInterest.PriceDelta5m, data.OpenInterest.PriceDelta15m, data.OpenInterest.PriceDelta1h, data.OpenInterest.PriceDelta4h))
	}

	if data.Microstructure != nil {
		sb.WriteString(fmt.Sprintf("Microstructure → CVD(1m/3m/15m): %.4f / %.4f / %.4f | OFI(1m/3m/15m): %.4f / %.4f / %.4f | OBI10: %.4f | MicroPrice: %.4f\n\n",
			data.Microstructure.CVD1m, data.Microstructure.CVD3m, data.Microstructure.CVD15m,
			data.Microstructure.OFI1m, data.Microstructure.OFI3m, data.Microstructure.OFI15m,
			data.Microstructure.OBI10, data.Microstructure.MicroPrice))
	}

	if data.IntradaySeries != nil {
		sb.WriteString("Intraday series (3‑minute intervals, oldest → latest):\n\n")

		if len(data.IntradaySeries.MidPrices) > 0 {
			sb.WriteString(fmt.Sprintf("Mid prices: %s\n\n", formatFloatSlice(data.IntradaySeries.MidPrices)))
		}

		if len(data.IntradaySeries.EMA20Values) > 0 {
			sb.WriteString(fmt.Sprintf("EMA indicators (20‑period): %s\n\n", formatFloatSlice(data.IntradaySeries.EMA20Values)))
		}

		if len(data.IntradaySeries.MACDValues) > 0 {
			sb.WriteString(fmt.Sprintf("MACD indicators: %s\n\n", formatFloatSlice(data.IntradaySeries.MACDValues)))
		}

		if len(data.IntradaySeries.RSI7Values) > 0 {
			sb.WriteString(fmt.Sprintf("RSI indicators (7‑Period): %s\n\n", formatFloatSlice(data.IntradaySeries.RSI7Values)))
		}

		if len(data.IntradaySeries.RSI14Values) > 0 {
			sb.WriteString(fmt.Sprintf("RSI indicators (14‑Period): %s\n\n", formatFloatSlice(data.IntradaySeries.RSI14Values)))
		}
	}

	if data.LongerTermContext != nil {
		sb.WriteString("Longer‑term context (4‑hour timeframe):\n\n")

		sb.WriteString(fmt.Sprintf("20‑Period EMA: %.3f vs. 50‑Period EMA: %.3f\n\n",
			data.LongerTermContext.EMA20, data.LongerTermContext.EMA50))

		sb.WriteString(fmt.Sprintf("3‑Period ATR: %.3f vs. 14‑Period ATR: %.3f\n\n",
			data.LongerTermContext.ATR3, data.LongerTermContext.ATR14))

		sb.WriteString(fmt.Sprintf("Current Volume: %.3f vs. Average Volume: %.3f\n\n",
			data.LongerTermContext.CurrentVolume, data.LongerTermContext.AverageVolume))

		if len(data.LongerTermContext.MACDValues) > 0 {
			sb.WriteString(fmt.Sprintf("MACD indicators: %s\n\n", formatFloatSlice(data.LongerTermContext.MACDValues)))
		}

		if len(data.LongerTermContext.RSI14Values) > 0 {
			sb.WriteString(fmt.Sprintf("RSI indicators (14‑Period): %s\n\n", formatFloatSlice(data.LongerTermContext.RSI14Values)))
		}
	}

	return sb.String()
}

// formatFloatSlice 格式化float64切片为字符串
func formatFloatSlice(values []float64) string {
	strValues := make([]string, len(values))
	for i, v := range values {
		strValues[i] = fmt.Sprintf("%.3f", v)
	}
	return "[" + strings.Join(strValues, ", ") + "]"
}

type aggTrade struct {
	Quantity     float64
	Price        float64
	BuyerIsMaker bool
	Timestamp    int64
}

type orderBookSnapshot struct {
	Bids [][2]float64
	Asks [][2]float64
}

func getMicrostructureData(symbol string) *MicrostructureData {
	data := &MicrostructureData{}

	now := time.Now().UnixMilli()

	if trades, err := getAggTrades(symbol, now-60*1000); err == nil {
		data.CVD1m, data.OFI1m = aggregateFlow(trades)
	}

	if trades, err := getAggTrades(symbol, now-3*60*1000); err == nil {
		data.CVD3m, data.OFI3m = aggregateFlow(trades)
	}

	if trades, err := getAggTrades(symbol, now-15*60*1000); err == nil {
		data.CVD15m, data.OFI15m = aggregateFlow(trades)
	}

	if depth, err := getOrderBook(symbol, 10); err == nil {
		data.OBI10 = calculateOrderBookImbalance(depth)
		data.MicroPrice = calculateMicroPrice(depth)
	}

	return data
}

func getAggTrades(symbol string, startTime int64) ([]aggTrade, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/aggTrades?symbol=%s&startTime=%d&limit=1000", symbol, startTime)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw []struct {
		Price        string `json:"p"`
		Quantity     string `json:"q"`
		BuyerIsMaker bool   `json:"m"`
		Timestamp    int64  `json:"T"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	trades := make([]aggTrade, 0, len(raw))
	for _, item := range raw {
		qty, err := strconv.ParseFloat(item.Quantity, 64)
		if err != nil {
			continue
		}
		price, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			continue
		}
		trades = append(trades, aggTrade{
			Quantity:     qty,
			Price:        price,
			BuyerIsMaker: item.BuyerIsMaker,
			Timestamp:    item.Timestamp,
		})
	}

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp < trades[j].Timestamp
	})

	return trades, nil
}

func aggregateFlow(trades []aggTrade) (float64, float64) {
	if len(trades) == 0 {
		return 0, 0
	}

	buyVol := 0.0
	sellVol := 0.0
	for _, t := range trades {
		if t.BuyerIsMaker {
			sellVol += t.Quantity
		} else {
			buyVol += t.Quantity
		}
	}

	cvd := buyVol - sellVol
	total := buyVol + sellVol
	if total == 0 {
		return cvd, 0
	}

	ofi := (buyVol - sellVol) / total
	return cvd, ofi
}

func getOrderBook(symbol string, limit int) (*orderBookSnapshot, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/depth?symbol=%s&limit=%d", symbol, limit)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	snapshot := &orderBookSnapshot{}
	for _, bid := range raw.Bids {
		if len(bid) < 2 {
			continue
		}
		price, err1 := strconv.ParseFloat(bid[0], 64)
		qty, err2 := strconv.ParseFloat(bid[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		snapshot.Bids = append(snapshot.Bids, [2]float64{price, qty})
	}

	for _, ask := range raw.Asks {
		if len(ask) < 2 {
			continue
		}
		price, err1 := strconv.ParseFloat(ask[0], 64)
		qty, err2 := strconv.ParseFloat(ask[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		snapshot.Asks = append(snapshot.Asks, [2]float64{price, qty})
	}

	return snapshot, nil
}

func calculateOrderBookImbalance(book *orderBookSnapshot) float64 {
	if book == nil || len(book.Bids) == 0 || len(book.Asks) == 0 {
		return 0
	}

	maxDepth := len(book.Bids)
	if len(book.Asks) < maxDepth {
		maxDepth = len(book.Asks)
	}

	sumBids := 0.0
	sumAsks := 0.0
	for i := 0; i < maxDepth; i++ {
		sumBids += book.Bids[i][1]
		sumAsks += book.Asks[i][1]
	}

	total := sumBids + sumAsks
	if total == 0 {
		return 0
	}

	return (sumBids - sumAsks) / total
}

func calculateMicroPrice(book *orderBookSnapshot) float64 {
	if book == nil || len(book.Bids) == 0 || len(book.Asks) == 0 {
		return 0
	}

	bestBid := book.Bids[0]
	bestAsk := book.Asks[0]
	denom := bestBid[1] + bestAsk[1]
	if denom == 0 {
		return (bestBid[0] + bestAsk[0]) / 2
	}

	return (bestAsk[0]*bestBid[1] + bestBid[0]*bestAsk[1]) / denom
}

// Normalize 标准化symbol,确保是USDT交易对
func Normalize(symbol string) string {
	symbol = strings.ToUpper(symbol)
	if strings.HasSuffix(symbol, "USDT") {
		return symbol
	}
	return symbol + "USDT"
}

// parseFloat 解析float值
func parseFloat(v interface{}) (float64, error) {
	switch val := v.(type) {
	case string:
		return strconv.ParseFloat(val, 64)
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", v)
	}
}
