# Практика 0

## Окружение

- Сделать директорию для всего курса
- Каждая домашка в своей поддиректории
- В рутовой диреткории

```
go work init
mkdir practice0
cd practice0
go mod init practice0
cd ..
go work use practice0
```

## Техническое задание

Сделать функцию

- `func filler(b []byte, ifzero byte, ifnot byte)`.

Пусть она вызывает в цикле рандом в интервале от 0 до 2, и если 0 заполняет в байт слайса `ifzero`, а если 1, то `ifnot`.

## Тот самый TDD

Создадим пустую функцию, которую будем тестировать.

- `main.go`

```golang
package main

func main() {
}

func filler(b []byte, ifzero byte, ifnot byte) {
}
```

Создадим модуль с тестом

- `main_test.go`

```golang
package main

import (
	"slices"
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	// Заполнить здесь ассерт, что b содержит zero и что b содержит one
}
```

- Запуск тестов

```
go test main.go main_test.go
```

Тесты должны быть красные:

```
FAIL
FAIL	command-line-arguments	0.306s
FAIL
```

Сделаем так, чтобы тесты показывали, где именно происходит ассерт.

Наполним функцию правильным кодом и добьемся того, чтобы тесты позеленели.

## Теперь основное приложение

Сделаем одну горутину, которая заполняет _первую_ половинку массива с помощью `filler` с параметрами
`ifzero = '0'`, `ifnot = '1'`. И ждет 1 секунду.

Сделаем вторую горутину, которая заполняет _вторую_ половинку массива с помощью `filler` с параметрами
`ifzero = 'X'`, `ifnot = 'Y'`. И ждет 1 секунду.

Пусть третья горутина в цикле выводит массив символов с помощью `fmt.Println`.
И ждет 1 секунду.

Пусть основная ветка программы зациклится навсегда.

В результате должна получиться программа

## Вопросы

- Хороший ли получился тест?
- Какими примитивами синхронизации надо пользоваться?
- Как выйти из программы?
- Почему выход из программы работает?
